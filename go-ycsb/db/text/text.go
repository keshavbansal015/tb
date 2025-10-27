package text

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/prop"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

type textDB struct {
	writer     *bufio.Writer
	outputDir  string
	fieldCount int64
}

func (t *textDB) Close() error {
	return t.writer.Flush()
}

func (t *textDB) InitThread(ctx context.Context, _ int, _ int) context.Context {
	return ctx
}

func (t *textDB) CleanupThread(_ context.Context) {
	if t.writer != nil {
		t.writer.Flush()
	}
}

func (t *textDB) Read(_ context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	// Not implemented for load phase
	line := "GET " + key + "\n"
	_, err := t.writer.WriteString(line)
	if err != nil {
		return nil, err
	}
	t.writer.Flush()

	return nil, nil
}

func (t *textDB) Scan(_ context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	// Not implemented
	return nil, fmt.Errorf("scan is not supported")
}

func (t *textDB) Update(_ context.Context, table string, key string, values map[string][]byte) error {
	// Not implemented for load phase
	line := "SET " + key + " "
	for _, value := range values {
		line += string(value)
	}
	line += "\n"
	_, err := t.writer.WriteString(line)
	if err != nil {
		return err
	}
	t.writer.Flush()

	return nil
}

func (t *textDB) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	// Write to file in YCSB text format: key\tfield0=value0\tfield1=value1...
	line := "SET " + key
	for _, value := range values {
		line += " " + string(value)
	}
	line += "\n"

	_, err := t.writer.WriteString(line)
	if err != nil {
		return err
	}
	return t.writer.Flush()
}

func (t *textDB) Delete(_ context.Context, table string, key string) error {
	// Not implemented
	return nil
}

type textCreator struct{}

func (t textCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	// Print all properties in p
	tableName, _ := p.Get("table")
	//fmt.Printf("Properties: %v", table_)
	//fmt.Printf("Properties: %v", p)
	txt := &textDB{}

	// Get output directory from properties
	txt.outputDir = p.GetString("text.output.dir", "./")

	// Create output directory
	if err := os.MkdirAll(txt.outputDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create output directory %s: %v", txt.outputDir, err)
	}

	// Open the file for writing (append mode for multi-threaded load)

	//filePath := filepath.Join(txt.outputDir, prop.table)

	filePath := filepath.Join(txt.outputDir, tableName)
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %v", filePath, err)
	}

	txt.writer = bufio.NewWriter(file)
	txt.fieldCount = p.GetInt64(prop.FieldCount, prop.FieldCountDefault)

	// Optional: Drop data (truncate file if exists)
	if p.GetBool(prop.DropData, prop.DropDataDefault) {
		if err := file.Truncate(0); err != nil {
			return nil, fmt.Errorf("failed to truncate file %s: %v", filePath, err)
		}
	}

	return txt, nil
}

func init() {
	ycsb.RegisterDBCreator("text", textCreator{})
}
