package mongodb

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/lakesense/lakesense/engine/internal/model"
	"github.com/lakesense/lakesense/engine/internal/sdk"
)

func TestBsonType(t *testing.T) {
	oid := primitive.NewObjectID()
	require.Equal(t, model.TypeString, bsonType(oid))
	require.Equal(t, model.TypeString, bsonType("s"))
	require.Equal(t, model.TypeInt32, bsonType(int32(1)))
	require.Equal(t, model.TypeInt64, bsonType(int64(1)))
	require.Equal(t, model.TypeFloat64, bsonType(1.5))
	require.Equal(t, model.TypeBool, bsonType(true))
	require.Equal(t, model.TypeJSON, bsonType(bson.D{{Key: "a", Value: 1}}))
	require.Equal(t, model.TypeJSON, bsonType(bson.A{1, 2}))
}

func TestDocToRowFlattensNested(t *testing.T) {
	oid := primitive.NewObjectID()
	doc := bson.D{
		{Key: "_id", Value: oid},
		{Key: "name", Value: "alice"},
		{Key: "tags", Value: bson.A{"a", "b"}},
		{Key: "profile", Value: bson.D{{Key: "city", Value: "NYC"}}},
	}
	row := docToRow(doc)
	require.Equal(t, oid.Hex(), row["_id"], "ObjectID → hex string")
	require.Equal(t, "alice", row["name"])
	require.Equal(t, `["a","b"]`, row["tags"], "array → JSON")
	require.Equal(t, `{"city":"NYC"}`, row["profile"], "nested doc → JSON")
}

func TestSpecCapabilities(t *testing.T) {
	require.NoError(t, sdk.ValidateCapabilities(New()))
	spec := New().Spec()
	require.Equal(t, "mongodb", spec.Type)
	require.Contains(t, spec.Capabilities, sdk.CapFullLoad)
	require.Contains(t, spec.Capabilities, sdk.CapIncremental)
}
