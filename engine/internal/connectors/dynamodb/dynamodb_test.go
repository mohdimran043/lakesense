package dynamodb

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"

	"github.com/lakesense/lakesense/engine/internal/model"
	"github.com/lakesense/lakesense/engine/internal/sdk"
)

func TestConfigValidateDefaults(t *testing.T) {
	c := &Config{}
	require.NoError(t, c.validate())
	require.Equal(t, "us-east-1", c.Region)
	require.Equal(t, int32(100), c.SampleSize)

	require.Error(t, (&Config{Type: "redis"}).validate()) // wrong type
}

func TestScalarType(t *testing.T) {
	require.Equal(t, model.TypeString, scalarType(types.ScalarAttributeTypeS))
	require.Equal(t, model.TypeDecimal, scalarType(types.ScalarAttributeTypeN))
	require.Equal(t, model.TypeBinary, scalarType(types.ScalarAttributeTypeB))
}

func TestAttrType(t *testing.T) {
	require.Equal(t, model.TypeString, attrType(&types.AttributeValueMemberS{Value: "x"}))
	require.Equal(t, model.TypeDecimal, attrType(&types.AttributeValueMemberN{Value: "1"}))
	require.Equal(t, model.TypeBool, attrType(&types.AttributeValueMemberBOOL{Value: true}))
	require.Equal(t, model.TypeBinary, attrType(&types.AttributeValueMemberB{Value: []byte("b")}))
	require.Equal(t, model.TypeJSON, attrType(&types.AttributeValueMemberM{}))
	require.Equal(t, model.TypeJSON, attrType(&types.AttributeValueMemberL{}))
	require.Equal(t, model.TypeString, attrType(&types.AttributeValueMemberNULL{Value: true}))
}

func TestMergeType(t *testing.T) {
	require.Equal(t, model.TypeDecimal, mergeType("", model.TypeDecimal)) // first sighting
	require.Equal(t, model.TypeDecimal, mergeType(model.TypeDecimal, model.TypeDecimal))
	require.Equal(t, model.TypeString, mergeType(model.TypeDecimal, model.TypeString)) // conflict → string
	require.Equal(t, model.TypeString, mergeType(model.TypeJSON, model.TypeBool))
}

func TestAttrValueScalarsAndNesting(t *testing.T) {
	require.Equal(t, "hello", attrValue(&types.AttributeValueMemberS{Value: "hello"}))
	// A big integer keeps its exact text — no float rounding.
	require.Equal(t, "90071992547409910", attrValue(&types.AttributeValueMemberN{Value: "90071992547409910"}))
	require.Equal(t, true, attrValue(&types.AttributeValueMemberBOOL{Value: true}))
	require.Nil(t, attrValue(&types.AttributeValueMemberNULL{Value: true}))

	// A nested map serializes to JSON, numbers stay bare.
	m := &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{
		"n": &types.AttributeValueMemberN{Value: "5"},
		"s": &types.AttributeValueMemberS{Value: "a"},
	}}
	require.JSONEq(t, `{"n":5,"s":"a"}`, attrValue(m).(string))

	// A string set serializes as a JSON array.
	ss := &types.AttributeValueMemberSS{Value: []string{"x", "y"}}
	require.JSONEq(t, `["x","y"]`, attrValue(ss).(string))
}

func TestItemToRow(t *testing.T) {
	row := itemToRow(map[string]types.AttributeValue{
		"id":  &types.AttributeValueMemberS{Value: "u1"},
		"age": &types.AttributeValueMemberN{Value: "30"},
		"vip": &types.AttributeValueMemberBOOL{Value: true},
	})
	require.Equal(t, "u1", row["id"])
	require.Equal(t, "30", row["age"])
	require.Equal(t, true, row["vip"])
}

func TestSpecCapabilities(t *testing.T) {
	require.NoError(t, sdk.ValidateCapabilities(New()))
	spec := New().Spec()
	require.Equal(t, "dynamodb", spec.Type)
	require.Equal(t, sdk.MaturityBeta, spec.Maturity)
	require.Equal(t, []sdk.Capability{sdk.CapFullLoad}, spec.Capabilities)
}
