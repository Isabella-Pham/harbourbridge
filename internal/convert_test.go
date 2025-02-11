// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package internal

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
)

// This file contains very basic tests of Conv API functionality.
// Most of the Conv APIs are also tested in process_test.go (where
// they are tested using data from schema/data conversion).

func TestSetSchemaMode(t *testing.T) {
	conv := MakeConv()
	conv.SetSchemaMode()
	assert.True(t, conv.SchemaMode())
	assert.False(t, conv.DataMode())
}

func TestSetDataMode(t *testing.T) {
	conv := MakeConv()
	conv.SetDataMode()
	assert.False(t, conv.SchemaMode())
	assert.True(t, conv.DataMode())
}

func TestRows(t *testing.T) {
	conv := MakeConv()
	conv.Stats.Rows["table1"] = 42
	conv.Stats.Rows["table2"] = 6
	assert.Equal(t, int64(48), conv.Rows())
}

func TestBadRows(t *testing.T) {
	conv := MakeConv()
	conv.Stats.BadRows["table1"] = 6
	conv.Stats.BadRows["table2"] = 2
	assert.Equal(t, int64(8), conv.BadRows())
}

func TestStatements(t *testing.T) {
	conv := MakeConv()
	conv.ErrorInStatement("Error statement")
	conv.SchemaStatement("Schema statement")
	conv.DataStatement("Data statement")
	conv.SkipStatement("Skip statement")
	assert.Equal(t, int64(4), conv.Statements())
}

func TestStatementErrors(t *testing.T) {
	conv := MakeConv()
	conv.ErrorInStatement("Error statement")
	assert.Equal(t, int64(1), conv.StatementErrors())
}

func TestUnexpecteds(t *testing.T) {
	conv := MakeConv()
	conv.Unexpected("expected-the-unexpected")
	assert.Equal(t, int64(1), conv.Unexpecteds())
}

func TestGetBadRows(t *testing.T) {
	conv := MakeConv()
	row1 := row{"table", []string{"col1", "col2"}, []string{"a", "1"}}
	row2 := row{"table", []string{"col1"}, []string{"bb"}}
	conv.sampleBadRows.rows = []*row{&row1, &row2}
	assert.Equal(t, 2, len(conv.SampleBadRows(100)))
}

func TestAddPrimaryKeys(t *testing.T) {
	conv := MakeConv()
	conv.SpSchema["table"] = ddl.CreateTable{
		Name:     "table",
		ColNames: []string{"a", "b"},
		ColDefs: map[string]ddl.ColumnDef{
			"a": {Name: "a", T: ddl.Type{Name: ddl.Int64}},
			"b": {Name: "b", T: ddl.Type{Name: ddl.Float64}},
		},
		Pks: []ddl.IndexKey{}}
	conv.AddPrimaryKeys()
	e := ddl.CreateTable{
		Name:     "table",
		ColNames: []string{"a", "b", "synth_id"},
		ColDefs: map[string]ddl.ColumnDef{
			"a":        {Name: "a", T: ddl.Type{Name: ddl.Int64}},
			"b":        {Name: "b", T: ddl.Type{Name: ddl.Float64}},
			"synth_id": {Name: "synth_id", T: ddl.Type{Name: ddl.Int64}},
		},
		Pks: []ddl.IndexKey{{Col: "synth_id"}}}
	assert.Equal(t, e, conv.SpSchema["table"])
	assert.Equal(t, SyntheticPKey{Col: "synth_id", Sequence: 0}, conv.SyntheticPKeys["table"])
}
