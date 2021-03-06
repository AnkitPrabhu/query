//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package expression

import (
	"encoding/json"
	"reflect"

	"github.com/couchbase/query/auth"
	"github.com/couchbase/query/value"
)

/*
ExpressionBase is a base class for all expressions.
*/
type ExpressionBase struct {
	expr        Expression
	value       *value.Value
	conditional bool
	volatile    bool
	isVar       bool
}

var _NIL_VALUE value.Value

type Covered int

const (
	CoveredFalse    = Covered(iota) // not covered
	CoveredContinue                 // covering state can't be established yet, currently unused
	CoveredSkip                     // expression not relevant for covering, skip to next
	CoveredEquiv                    // expression is covered, ignore the rest
	CoveredTrue                     // covered
)

type coveredOptions struct {
	isSingle     bool
	skip         bool
	trickleEquiv bool
}

func (this *ExpressionBase) String() string {
	return NewStringer().Visit(this.expr)
}

func (this *ExpressionBase) MarshalJSON() ([]byte, error) {
	s := NewStringer().Visit(this.expr)
	return json.Marshal(s)
}

/*
Evaluate the expression for an indexing context. Support multiple
return values for array indexing.

By default, just call Evaluate().
*/
func (this *ExpressionBase) EvaluateForIndex(item value.Value, context Context) (
	value.Value, value.Values, error) {
	val, err := this.expr.Evaluate(item, context)
	return val, nil, err
}

/*
This method indicates if the expression is an array index key, and
if so, whether it is distinct.
*/

func (this *ExpressionBase) IsArrayIndexKey() (bool, bool) {
	return false, false
}

/*
This method indicates if the expression is a collection variable
*/
func (this *ExpressionBase) IsCollectionVariable() bool {
	return this.isVar
}

func (this *ExpressionBase) SetCollectionVariable() {
	this.isVar = true
}

/*
Value() returns the static / constant value of this Expression, or
nil. Expressions that depend on data, clocks, or random numbers must
return nil.
*/
func (this *ExpressionBase) Value() value.Value {
	if this.value != nil {
		return *this.value
	}

	if this.volatile {
		this.value = &_NIL_VALUE
		return nil
	}

	propMissing := this.expr.PropagatesMissing()
	propNull := this.expr.PropagatesNull()

	for _, child := range this.expr.Children() {
		cv := child.Value()
		if cv == nil {
			if this.value == nil {
				this.value = &_NIL_VALUE
			}

			continue
		}

		if propMissing && cv.Type() == value.MISSING {
			this.value = &cv
			return *this.value
		}

		if propNull && cv.Type() == value.NULL {
			this.value = &cv
		}
	}

	if this.value != nil {
		return *this.value
	}

	defer func() {
		err := recover()
		if err != nil {
			this.value = &_NIL_VALUE
		}
	}()

	val, err := this.expr.Evaluate(nil, nil)
	if err != nil {
		this.value = &_NIL_VALUE
		return nil
	}

	this.value = &val
	return *this.value
}

func (this *ExpressionBase) Static() Expression {
	for _, child := range this.expr.Children() {
		if child.Static() == nil {
			return nil
		}
	}

	return this.expr
}

/*
It returns an empty string or the terminal identifier of
the expression.
*/
func (this *ExpressionBase) Alias() string {
	return ""
}

/*
Range over the children of the expression, and check if each
child is indexable. If not then return false as the expression
is not indexable. If all children are indexable, then return
true.
*/
func (this *ExpressionBase) Indexable() bool {
	for _, child := range this.expr.Children() {
		if !child.Indexable() {
			return false
		}
	}

	return true
}

/*
Returns false if any child's PropagatesMissing() returns false.
*/
func (this *ExpressionBase) PropagatesMissing() bool {
	if this.conditional {
		return false
	}

	for _, child := range this.expr.Children() {
		if !child.PropagatesMissing() {
			return false
		}
	}

	return true
}

/*
Returns false if any child's PropagatesNull() returns false.
*/
func (this *ExpressionBase) PropagatesNull() bool {
	if this.conditional {
		return false
	}

	for _, child := range this.expr.Children() {
		if !child.PropagatesNull() {
			return false
		}
	}

	return true
}

/*
Indicates if this expression is equivalent to the other expression.
False negatives are allowed. Used in index selection.
*/
func (this *ExpressionBase) EquivalentTo(other Expression) bool {
	if this.valueEquivalentTo(other) {
		return true
	}

	if reflect.TypeOf(this.expr) != reflect.TypeOf(other) {
		return false
	}

	ours := this.expr.Children()
	theirs := other.Children()

	if len(ours) != len(theirs) {
		return false
	}

	for i, child := range ours {
		if !child.EquivalentTo(theirs[i]) {
			return false
		}
	}

	return true
}

/*
Indicates if this expression depends on the other expression.  False
negatives are allowed. Used in index selection.
*/
func (this *ExpressionBase) DependsOn(other Expression) bool {
	if this.conditional || other.Value() != nil {
		return false
	}
	return this.dependsOn(other)
}

func (this *ExpressionBase) dependsOn(other Expression) bool {
	if this.expr.EquivalentTo(other) {
		return true
	}

	for _, child := range this.expr.Children() {
		if child.DependsOn(other) {
			return true
		}
	}

	return false
}

/*
Indicates if this expression is based on the keyspace and is covered
by the list of expressions; that is, this expression does not depend
on any stored data beyond the expressions.
*/
func (this *ExpressionBase) CoveredBy(keyspace string, exprs Expressions, options coveredOptions) Covered {
	for _, expr := range exprs {
		if this.expr.EquivalentTo(expr) {
			return CoveredTrue
		}
	}
	children := this.expr.Children()
	options.isSingle = len(children) == 1
	rv := CoveredTrue

	// MB-22112: we treat the special case where a keyspace is part of the projection list
	// a keyspace as a single term does not cover by definition
	// a keyspace as part of a field or a path does cover to delay the decision in terms
	// further down the path
	for _, child := range children {
		switch child.CoveredBy(keyspace, exprs, options) {
		case CoveredFalse:
			return CoveredFalse

		// MB-25317: ignore expressions not related to this keyspace
		case CoveredSkip:
			options.skip = true

		// MB-25650: this subexpression is already covered, no need to check subsequent terms
		case CoveredEquiv:
			options.skip = true

			// trickle down CoveredEquiv to outermost field
			if options.trickleEquiv {
				rv = CoveredEquiv
			}
		}
	}

	return rv
}

/*
If this expression is in the WHERE clause of a partial index, lists
the Expressions that are implicitly covered.
*/
func (this *ExpressionBase) FilterCovers(covers map[string]value.Value) map[string]value.Value {
	return covers
}

func (this *ExpressionBase) valueEquivalentTo(other Expression) bool {
	thisValue := this.expr.Value()
	otherValue := other.Value()

	return thisValue != nil && otherValue != nil &&
		thisValue.EquivalentTo(otherValue)
}

/*
Set the receiver expression to the input expression.
*/
func (this *ExpressionBase) SetExpr(expr Expression) {
	if this.expr == nil {
		this.expr = expr
	}
}

/*
Return TRUE if any child may overlap spans.
*/
func (this *ExpressionBase) MayOverlapSpans() bool {
	for _, child := range this.expr.Children() {
		if child.MayOverlapSpans() {
			return true
		}
	}

	return false
}

func (this *ExpressionBase) SurvivesGrouping(groupKeys Expressions, allowed *value.ScopeValue) (
	bool, Expression) {
	for _, key := range groupKeys {
		if this.expr.EquivalentTo(key) {
			return true, nil
		}
	}

	for _, child := range this.expr.Children() {
		ok, expr := child.SurvivesGrouping(groupKeys, allowed)
		if !ok {
			return ok, expr
		}
	}

	return true, nil
}

func (this *ExpressionBase) Privileges() *auth.Privileges {
	// By default, the privileges required for an expression are the union
	// of the privilges required for the children of the expression.
	children := this.expr.Children()
	if len(children) == 0 {
		return auth.NewPrivileges()
	} else if len(children) == 1 {
		return children[0].Privileges()
	}

	// We want to be careful here to avoid unnecessary allocation of auth.Privileges records.
	privilegeList := make([]*auth.Privileges, len(children))
	for i, child := range children {
		privilegeList[i] = child.Privileges()
	}

	totalPrivileges := 0
	for _, privs := range privilegeList {
		totalPrivileges += privs.Num()
	}

	if totalPrivileges == 0 {
		return privilegeList[0] // will be empty
	}

	unionPrivileges := auth.NewPrivileges()
	for _, privs := range privilegeList {
		unionPrivileges.AddAll(privs)
	}
	return unionPrivileges
}

/*
Return FALSE if any child is not IndexAggregatable()
*/

func (this *ExpressionBase) IndexAggregatable() bool {
	for _, child := range this.expr.Children() {
		if !child.IndexAggregatable() {
			return false
		}
	}

	return true
}

/*
Used for Xattr paths
*/

func (this *ExpressionBase) FieldNames(base Expression, names map[string]bool) (present bool) {
	present = false
	if Equivalent(base, this.expr) {
		return true
	}

	for _, child := range this.expr.Children() {
		if child.FieldNames(base, names) {
			present = true
		}
	}

	return present
}

func XattrsNames(exprs Expressions, alias string) (present bool, names []string) {
	present = false
	var xattrs Expression
	if alias == "" {
		xattrs = NewField(NewMeta(), NewFieldName("xattrs", false))
	} else {
		xattrs = NewField(NewMeta(NewIdentifier(alias)),
			NewFieldName("xattrs", false))
	}

	mNames := make(map[string]bool, 5)
	for _, expr := range exprs {
		if expr.FieldNames(xattrs, mNames) {
			present = true
		}
	}
	if len(mNames) > 0 {
		names = make([]string, 0, len(mNames))
		for s, _ := range mNames {
			if s == "$document" {
				names = append([]string{s}, names...)
			} else {
				names = append(names, s)
			}
		}
		return present, names
	}
	return present, nil
}

func (this *ExpressionBase) ResetValue() {
	this.value = nil
	for _, child := range this.expr.Children() {
		child.ResetValue()
	}
}
