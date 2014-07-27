//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package plan

import (
	"github.com/couchbaselabs/query/datastore"
	"github.com/couchbaselabs/query/expression"
)

type PrimaryScan struct {
	readonly
	index datastore.PrimaryIndex
}

func NewPrimaryScan(index datastore.PrimaryIndex) *PrimaryScan {
	return &PrimaryScan{
		index: index,
	}
}

func (this *PrimaryScan) Accept(visitor Visitor) (interface{}, error) {
	return visitor.VisitPrimaryScan(this)
}

func (this *PrimaryScan) Index() datastore.PrimaryIndex {
	return this.index
}

type IndexScan struct {
	readonly
	index    datastore.Index
	spans    datastore.Spans
	distinct bool
	limit    int64
}

func NewIndexScan(index datastore.Index, spans datastore.Spans, distinct bool, limit int64) *IndexScan {
	return &IndexScan{
		index:    index,
		spans:    spans,
		distinct: distinct,
		limit:    limit,
	}
}

func (this *IndexScan) Index() datastore.Index {
	return this.index
}

func (this *IndexScan) Spans() datastore.Spans {
	return this.spans
}

func (this *IndexScan) Distinct() bool {
	return this.distinct
}

func (this *IndexScan) Limit() int64 {
	return this.limit
}

// KeyScan is used for KEYS clauses (except after JOIN / NEST).
type KeyScan struct {
	readonly
	keys expression.Expression
}

func NewKeyScan(keys expression.Expression) *KeyScan {
	return &KeyScan{
		keys: keys,
	}
}

func (this *KeyScan) Accept(visitor Visitor) (interface{}, error) {
	return visitor.VisitKeyScan(this)
}

func (this *KeyScan) Keys() expression.Expression {
	return this.keys
}

// ParentScan is used for UNNEST subqueries.
type ParentScan struct {
	readonly
}

func NewParentScan() *ParentScan {
	return &ParentScan{}
}

func (this *ParentScan) Accept(visitor Visitor) (interface{}, error) {
	return visitor.VisitParentScan(this)
}

// ValueScan is used for VALUES clauses, e.g. in INSERTs.
type ValueScan struct {
	readonly
	values expression.Expression
}

func NewValueScan(values expression.Expression) *ValueScan {
	return &ValueScan{
		values: values,
	}
}

func (this *ValueScan) Accept(visitor Visitor) (interface{}, error) {
	return visitor.VisitValueScan(this)
}

func (this *ValueScan) Values() expression.Expression {
	return this.values
}

// DummyScan is used for SELECTs with no FROM clause.
type DummyScan struct {
	readonly
}

func NewDummyScan() *DummyScan {
	return &DummyScan{}
}

func (this *DummyScan) Accept(visitor Visitor) (interface{}, error) {
	return visitor.VisitDummyScan(this)
}

// CountScan is used for SELECT COUNT(*) with no WHERE clause.
type CountScan struct {
	readonly
	keyspace datastore.Keyspace
}

func NewCountScan(keyspace datastore.Keyspace) *CountScan {
	return &CountScan{
		keyspace: keyspace,
	}
}

func (this *CountScan) Accept(visitor Visitor) (interface{}, error) {
	return visitor.VisitCountScan(this)
}

func (this *CountScan) Keyspace() datastore.Keyspace {
	return this.keyspace
}

// IntersectScan scans multiple indexes and intersects the results.
type IntersectScan struct {
	readonly
	scans []Operator
}

func NewIntersectScan(scans ...Operator) *IntersectScan {
	return &IntersectScan{
		scans: scans,
	}
}

func (this *IntersectScan) Accept(visitor Visitor) (interface{}, error) {
	return visitor.VisitIntersectScan(this)
}

func (this *IntersectScan) Scans() []Operator {
	return this.scans
}
