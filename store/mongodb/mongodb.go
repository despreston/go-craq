// mongodb package is for using MongoDB as the storage layer for a CRAQ node.
package mongodb

import (
	"context"
	"log"

	"github.com/despreston/go-craq/store"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const collName = "items"

type item struct {
	Version   uint64 `bson:"version"`
	Committed bool   `bson:"committed"`
	Value     []byte `bson:"value"`
	Key       string `bson:"key"`
}

type MongoDB struct {
	client *mongo.Client
	db     *mongo.Database
	coll   *mongo.Collection
}

func New(db string, opts ...*options.ClientOptions) (*MongoDB, error) {
	client, err := mongo.NewClient(opts...)
	if err != nil {
		log.Fatalf("Failed to create Mongo client: %+v", err)
		return nil, err
	}

	conn := client.Database(db)

	m := &MongoDB{
		client: client,
		db:     conn,
		coll:   conn.Collection(collName),
	}

	return m, nil
}

func (m *MongoDB) Connect(ctx context.Context) error {
	return m.client.Connect(ctx)
}

func (m *MongoDB) Disconnect(ctx context.Context) error {
	return m.client.Disconnect(ctx)
}

func (m *MongoDB) DropCollection(ctx context.Context) error {
	return m.coll.Drop(ctx)
}

func (m *MongoDB) Read(key string) (*store.Item, error) {
	var items []item

	limit := int64(1)

	opts := options.FindOptions{
		Sort:  bson.M{"version": 1},
		Limit: &limit,
	}

	res, err := m.coll.Find(context.TODO(), bson.M{"key": key}, &opts)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, store.ErrNotFound
		}
		return nil, err
	}

	if err := res.All(context.TODO(), &items); err != nil {
		return nil, err
	}

	if len(items) < 1 {
		return nil, store.ErrNotFound
	}

	if !items[0].Committed {
		return nil, store.ErrDirtyItem
	}

	si := store.Item(items[0])
	return &si, nil
}

func (m *MongoDB) Write(key string, val []byte, version uint64) error {
	itm := item{
		Version:   version,
		Committed: false,
		Value:     val,
		Key:       key,
	}

	_, err := m.coll.InsertOne(context.TODO(), itm)
	return err
}

func (m *MongoDB) Commit(key string, version uint64) error {
	// mark version committed
	filter := bson.M{"key": key, "version": version}
	update := bson.M{"$set": bson.M{"committed": true}}
	updateRes := m.coll.FindOneAndUpdate(context.TODO(), filter, update)
	if err := updateRes.Err(); err != nil {
		if err == mongo.ErrNoDocuments {
			return store.ErrNotFound
		}
	}

	// delete older versions
	older := bson.M{"key": key, "version": bson.M{"$lt": version}}
	_, err := m.coll.DeleteMany(context.TODO(), older)

	return err
}

func (m *MongoDB) ReadVersion(key string, version uint64) (*store.Item, error) {
	var itm item

	filter := bson.M{"key": key, "version": version}
	err := m.coll.FindOne(context.TODO(), filter).Decode(&itm)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, store.ErrNotFound
		}
		return nil, err
	}

	si := store.Item(itm)
	return &si, nil
}

func (m *MongoDB) AllNewerCommitted(verBykey map[string]uint64) ([]*store.Item, error) {
	filter := bson.M{"committed": true}
	or := bson.A{}
	keys := []string{}

	for k, v := range verBykey {
		f := bson.M{"key": k, "version": bson.M{"$gt": v}}
		or = append(or, f)
		keys = append(keys, k)
	}

	or = append(or, bson.M{"key": bson.M{"$nin": keys}})
	filter["$or"] = or

	var items []item

	res, err := m.coll.Find(context.TODO(), filter)
	if err != nil {
		return nil, err
	}

	if err := res.All(context.TODO(), &items); err != nil {
		return nil, err
	}

	si := make([]*store.Item, len(items))
	for i, _ := range items {
		si[i] = (*store.Item)(&items[i])
	}

	return si, nil
}

func (m *MongoDB) AllNewerDirty(verBykey map[string]uint64) ([]*store.Item, error) {
	filter := bson.M{"committed": false}
	or := bson.A{}
	keys := []string{}

	for k, v := range verBykey {
		f := bson.M{"key": k, "version": bson.M{"$gt": v}}
		or = append(or, f)
		keys = append(keys, k)
	}

	or = append(or, bson.M{"key": bson.M{"$nin": keys}})
	filter["$or"] = or

	var items []item

	res, err := m.coll.Find(context.TODO(), filter)
	if err != nil {
		return nil, err
	}

	if err := res.All(context.TODO(), &items); err != nil {
		return nil, err
	}

	si := make([]*store.Item, len(items))
	for i, _ := range items {
		si[i] = (*store.Item)(&items[i])
	}

	return si, nil
}

func (m *MongoDB) AllDirty() ([]*store.Item, error) {
	res, err := m.coll.Find(context.TODO(), bson.M{"committed": false})
	if err != nil {
		return nil, err
	}

	var items []item

	if err := res.All(context.TODO(), &items); err != nil {
		return nil, err
	}

	si := make([]*store.Item, len(items))
	for i, _ := range items {
		si[i] = (*store.Item)(&items[i])
	}

	return si, nil
}

func (m *MongoDB) AllCommitted() ([]*store.Item, error) {
	res, err := m.coll.Find(context.TODO(), bson.M{"committed": true})
	if err != nil {
		return nil, err
	}

	var items []item

	if err := res.All(context.TODO(), &items); err != nil {
		return nil, err
	}

	si := make([]*store.Item, len(items))
	for i, _ := range items {
		si[i] = (*store.Item)(&items[i])
	}

	return si, nil
}
