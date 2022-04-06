package main

import (
	"context"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"os"
)

func main() {
	opts := options.ChangeStream().
		SetFullDocument(options.Default)

	// Show new fields like collectionUUID and new change events type like create.
	opts.SetCustomPipeline(bson.M{"showExpandedEvents": true})

	//// If using the multiple replicator config, open a per-shard change stream.
	//if csr.isMultiReplicatorConfig {
	//	opts.SetCustom(bson.M{"$_passthroughToShard": bson.D{{"shard", csr.mongosyncID}}})
	//}
	//
	//// Resume change stream from the resume token if it exists, otherwise start change stream
	//// from the majority committed start timestamp of source cluster.
	//if csr.resumeToken != nil {
	//	csr.logger.Info().Msgf("Starting to watch change stream after resumeToken %v\n.", csr.resumeToken)
	//	opts = opts.SetStartAfter(csr.resumeToken)
	//} else if csr.startAtTs != nil {
	//	csr.logger.Info().Msgf("Starting to watch change stream from timestamp %v.", csr.startAtTs)
	//	opts = opts.SetStartAtOperationTime(csr.startAtTs)
	//} else {
	//	return nil, errors.Errorf("cannot start watching change stream without start timestamp or resume token")
	//}
	uri := "mongodb://sourceAdmin:sourcePass@localhost:27017/?authSource=admin"
	clientOpts := options.Client().ApplyURI(uri)

	client, err := mongo.NewClient(clientOpts)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	err = client.Connect(context.Background())
	if err != nil {
		fmt.Println("failed connect", err)
		os.Exit(1)
	}

	changeStream, driverErr := client.Watch(context.Background(), mongo.Pipeline{}, opts)
	if driverErr != nil {
		fmt.Println(driverErr)
		os.Exit(1)
	}

	for changeStream.Next(context.Background()) {
		event := changeStream.Current
		fmt.Printf("%+v\n", event)
	}
}
