package main

import (
	"bytes"
	"flag"
	"log"
	"os"

	opensearch "github.com/opensearch-project/opensearch-go/v2"
)

func main() {
	var (
		addr        = flag.String("addr", "http://localhost:9200", "OpenSearch address")
		template    = flag.String("template", "traces", "Index template name")
		mappingFile = flag.String("mapping", "configs/opensearch/traces_index_mapping.json", "Mapping JSON file")
	)
	flag.Parse()

	client, err := opensearch.NewClient(opensearch.Config{Addresses: []string{*addr}})
	if err != nil {
		log.Fatalf("failed to create client: %v", err)
	}

	body, err := os.ReadFile(*mappingFile)
	if err != nil {
		log.Fatalf("unable to read mapping file: %v", err)
	}

	res, err := client.Indices.PutIndexTemplate(*template, bytes.NewReader(body))
	if err != nil {
		log.Fatalf("failed to put template: %v", err)
	}
	if res.IsError() {
		log.Fatalf("OpenSearch error: %s", res.String())
	}
	log.Printf("template %s created", *template)
}
