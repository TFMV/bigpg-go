// --------------------------------------------------------------------------------
// Author: Thomas F McGeehan V
//
// This file is part of a software project developed by Thomas F McGeehan V.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
//
// For more information about the MIT License, please visit:
// https://opensource.org/licenses/MIT
//
// Acknowledgment appreciated but not required.
// --------------------------------------------------------------------------------

// Package utils provides common utilities for ArrowArc.
package utils

import (
	"errors"
	"fmt"
	"log"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

// PrintRecordBatch prints the contents of an Arrow record batch.
func PrintRecordBatch(record arrow.Record) error {
	if record == nil {
		return errors.New("record cannot be nil")
	}

	numRows := int(record.NumRows())
	numCols := int(record.NumCols())
	fmt.Printf("Record batch with %d rows and %d columns\n", numRows, numCols)

	for i := 0; i < numCols; i++ {
		field := record.Schema().Field(i)
		if field.Name == "" || field.Type == nil {
			return fmt.Errorf("invalid schema at column %d: field name or type is missing", i)
		}

		fmt.Printf("Column %d: %s, Value:%v, Type: %s\n", i, field.Name, record.Column(i), field.Type)
	}

	for rowIndex := 0; rowIndex < numRows; rowIndex++ {
		for colIndex := 0; colIndex < numCols; colIndex++ {
			column := record.Column(colIndex)
			if column == nil {
				fmt.Printf("Row %d, Column %d: Nil column data\n", rowIndex, colIndex)
				continue
			}
		}
	}

	return nil
}

// IpcReaderToChannel reads records from an IPC reader and sends them to a channel.
func IpcReaderToChannel(reader *ipc.Reader) (<-chan arrow.Record, error) {
	if reader == nil {
		return nil, errors.New("ipc.Reader cannot be nil")
	}

	recordChan := make(chan arrow.Record)

	go func() {
		defer close(recordChan)
		for reader.Next() {
			record := reader.Record()
			if record == nil {
				log.Println("Warning: Received nil record from IPC reader")
				continue
			}
			recordChan <- record
		}
	}()

	return recordChan, nil
}

// ArrowRecordToString converts an Arrow record to a string representation.
func ArrowRecordToString(record arrow.Record) (string, error) {
	if record == nil {
		return "", errors.New("record cannot be nil")
	}

	var result string
	for i := 0; i < int(record.NumRows()); i++ {
		for j := 0; j < int(record.NumCols()); j++ {
			column := record.Column(j)
			if column == nil {
				result += "Nil column "
				continue
			}
			switch col := column.(type) {
			case *array.String:
				result += fmt.Sprintf("%s ", col.Value(i))
			case *array.Float64:
				result += fmt.Sprintf("%.2f ", col.Value(i))
			default:
				result += fmt.Sprintf("Unknown type %T ", column)
			}
		}
		if i < int(record.NumRows())-1 {
			result += "\n" // Add a newline after each row except the last one
		}
	}
	return result, nil
}

// CloneSourceStream creates multiple clones of an Arrow record source channel.
func CloneSourceStream(sourceChan <-chan arrow.Record, numClones int) ([]<-chan arrow.Record, error) {
	if sourceChan == nil {
		return nil, errors.New("source channel cannot be nil")
	}
	if numClones <= 0 {
		return nil, errors.New("number of clones must be greater than zero")
	}

	clones := make([]<-chan arrow.Record, numClones)
	for i := 0; i < numClones; i++ {
		cloneChan := make(chan arrow.Record)
		clones[i] = cloneChan
		go func() {
			for record := range sourceChan {
				if record == nil {
					log.Println("Warning: Nil record encountered in source channel")
					continue
				}
				cloneChan <- record
			}
			close(cloneChan)
		}()
	}
	return clones, nil
}

// ArrowBatchToJSON converts an Arrow record batch to a JSON string.
func ArrowBatchToJSON(batch arrow.Record) (string, error) {
	if batch == nil {
		return "", errors.New("batch cannot be nil")
	}

	json, err := batch.MarshalJSON()
	if err != nil {
		return "", fmt.Errorf("failed to marshal Arrow batch to JSON: %w", err)
	}
	return string(json), nil
}

// ProcessStreams processes errors from read and write error channels concurrently.
func ProcessStreams(readErrChan <-chan error, writeErrChan <-chan error) error {
	if readErrChan == nil || writeErrChan == nil {
		return errors.New("read and write error channels cannot be nil")
	}

	var wg sync.WaitGroup
	var readErr, writeErr error

	wg.Add(2)

	go func() {
		defer wg.Done()
		for err := range readErrChan {
			if err != nil {
				readErr = fmt.Errorf("error while reading: %w", err)
				return
			}
		}
	}()

	go func() {
		defer wg.Done()
		for err := range writeErrChan {
			if err != nil {
				writeErr = fmt.Errorf("error while writing: %w", err)
				return
			}
		}
	}()

	wg.Wait()

	if readErr != nil {
		return readErr
	}
	if writeErr != nil {
		return writeErr
	}

	return nil
}
