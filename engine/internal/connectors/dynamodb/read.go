package dynamodb

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/lakesense/lakesense/engine/internal/model"
	"github.com/lakesense/lakesense/engine/internal/sdk"
	"github.com/lakesense/lakesense/engine/internal/state"
)

// SplitChunks implements sdk.FullLoader: a single chunk. Scan paginates the whole
// table within ReadChunk (parallel segment scans are a later refinement).
func (c *Connector) SplitChunks(_ context.Context, _ model.Stream) ([]state.Chunk, error) {
	if c.client == nil {
		return nil, fmt.Errorf("connector not set up")
	}
	return []state.Chunk{{}}, nil
}

// ReadChunk implements sdk.FullLoader: Scans every item of the table, paging via
// LastEvaluatedKey, and emits one row per item.
func (c *Connector) ReadChunk(ctx context.Context, stream model.Stream, _ state.Chunk, emit sdk.RowFunc) error {
	if c.client == nil {
		return fmt.Errorf("connector not set up")
	}
	var start map[string]types.AttributeValue
	for {
		res, err := c.client.Scan(ctx, &dynamodb.ScanInput{
			TableName:         aws.String(stream.Name),
			ExclusiveStartKey: start,
		})
		if err != nil {
			return fmt.Errorf("scan %s: %w", stream.Name, err)
		}
		for _, item := range res.Items {
			if err := emit(ctx, itemToRow(item)); err != nil {
				return err
			}
		}
		if len(res.LastEvaluatedKey) == 0 {
			break
		}
		start = res.LastEvaluatedKey
	}
	return nil
}

// itemToRow flattens a DynamoDB item into an engine row.
func itemToRow(item map[string]types.AttributeValue) sdk.Row {
	row := make(sdk.Row, len(item))
	for name, av := range item {
		row[name] = attrValue(av)
	}
	return row
}

// attrType maps a DynamoDB attribute value to a lake type for schema inference.
func attrType(av types.AttributeValue) model.DataType {
	switch av.(type) {
	case *types.AttributeValueMemberS:
		return model.TypeString
	case *types.AttributeValueMemberN:
		return model.TypeDecimal
	case *types.AttributeValueMemberB:
		return model.TypeBinary
	case *types.AttributeValueMemberBOOL:
		return model.TypeBool
	case *types.AttributeValueMemberNULL:
		return model.TypeString
	default: // M, L, SS, NS, BS → nested/collection
		return model.TypeJSON
	}
}

// attrValue converts a DynamoDB attribute value to the lake value emitted for a
// row: scalars stay scalar (numbers keep their exact text), everything nested or
// set-valued becomes a JSON string.
func attrValue(av types.AttributeValue) any {
	switch v := av.(type) {
	case *types.AttributeValueMemberS:
		return v.Value
	case *types.AttributeValueMemberN:
		return v.Value // exact numeric text (arbitrary precision)
	case *types.AttributeValueMemberB:
		return v.Value // []byte
	case *types.AttributeValueMemberBOOL:
		return v.Value
	case *types.AttributeValueMemberNULL:
		return nil
	default: // M, L, SS, NS, BS
		b, err := json.Marshal(plainValue(av))
		if err != nil {
			return fmt.Sprintf("%v", av)
		}
		return string(b)
	}
}

// plainValue converts an attribute value into a plain Go value suitable for JSON
// encoding, recursing through maps and lists so nested structure is preserved.
func plainValue(av types.AttributeValue) any {
	switch v := av.(type) {
	case *types.AttributeValueMemberS:
		return v.Value
	case *types.AttributeValueMemberN:
		return json.Number(v.Value) // marshals as a bare number, exact
	case *types.AttributeValueMemberBOOL:
		return v.Value
	case *types.AttributeValueMemberNULL:
		return nil
	case *types.AttributeValueMemberB:
		return v.Value // JSON-encodes as base64
	case *types.AttributeValueMemberM:
		m := make(map[string]any, len(v.Value))
		for k, x := range v.Value {
			m[k] = plainValue(x)
		}
		return m
	case *types.AttributeValueMemberL:
		l := make([]any, len(v.Value))
		for i, x := range v.Value {
			l[i] = plainValue(x)
		}
		return l
	case *types.AttributeValueMemberSS:
		return v.Value
	case *types.AttributeValueMemberNS:
		ns := make([]json.Number, len(v.Value))
		for i, s := range v.Value {
			ns[i] = json.Number(s)
		}
		return ns
	case *types.AttributeValueMemberBS:
		return v.Value // [][]byte → array of base64
	default:
		return fmt.Sprintf("%v", av)
	}
}
