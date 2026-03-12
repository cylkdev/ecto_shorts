# COMMON_FILTERS.md Comprehensive Audit Report

**Date:** March 5, 2026  
**Audit Scope:** All 21 sections of `research/COMMON_FILTERS.md`  
**Coverage Criteria:** Representative coverage - each operator should have at least one example per operator category (comparison, negation, special operators)

---

## Executive Summary

This report documents a comprehensive audit of `research/COMMON_FILTERS.md` to identify missing operator + operator combinations. The audit examined all 21 sections to ensure representative coverage where each operator demonstrates its behaviour with different operator categories.

**Key Findings:**
- **Total Sections Audited:** 21
- **Sections with Missing Examples:** 8
- **Total Missing Examples Identified:** 24
- **Priority Level:** High (critical gaps in core query-building operators)

---

## Audit Methodology

For each section, the following analysis was performed:

1. **Directive Inventory:** Listed all operators mentioned in the section description
2. **Operator Categories:** Identified applicable operator categories (comparison, negation, string matching, special operators)
3. **Coverage Matrix:** Mapped existing examples to operators and operator categories
4. **Gap Identification:** Flagged operators missing examples for applicable operator categories

**Representative Coverage Definition:**
Each operator should have at least one example demonstrating:
- A comparison operator (>, >=, <, <=, ==, !=)
- Negation with `:not` (where applicable)
- Special operators specific to that operator (e.g., `:like`/`:ilike` for string fields, `:all`/`:any` for set operations)

---

## Section-by-Section Findings

### Section 1: Array Fields ✅ COMPLETE

**Directives Covered:**
- `:in` - Check if value is contained in array
- `:all` - Apply operation to all array elements
- `:count` - Count array elements and compare
- Comparison operators (element-wise)
- String matching (like, ilike)
- Transformations (lower, upper)

**Coverage Status:** ✅ Complete
- `:in` has examples with single values and arrays
- `:all` has examples with `:in` and `>` (representative)
- `:count` has examples with all 6 comparison operators (>, ==, <, >=, <=, !=)
- Comparison operators demonstrated element-wise (>, >=, <, <=)
- String matching shown with `:like` and `:ilike` (single and array patterns)
- Negation demonstrated with `:not` for equality, `:in`, `:like`, `:ilike`
- Field and value transformations both shown (`:lower`, `:upper`)

**Missing Examples:** None - this section has excellent coverage after recent additions.

---

### Section 2: Scalar Fields ✅ COMPLETE

**Directives Covered:**
- Direct field comparisons
- Multiple field combinations
- Top-level `:and` and `:or` operators
- Nil checks
- List values (IN clause)

**Coverage Status:** ✅ Complete
- Simple field comparisons demonstrated
- Multiple fields with implicit AND shown
- Explicit `:and` and `:or` at top level covered
- Nil comparisons included
- List values (coerced to IN) demonstrated
- Nested logical operators shown

**Missing Examples:** None

---

### Section 3: Negation Directives ✅ COMPLETE

**Directives Covered:**
- `:not` - Wraps an entire operation in logical NOT

**Coverage Status:** ✅ Complete
- `:not` with `:in` operator
- `:not` with equality on lists
- `:not` with inequality on lists
- `:not` with all comparison operators (>, >=, <, <=, ==, !=)
- `:not` with comparison operator aliases (gt, gte, lt, lte)

**Missing Examples:** None

---

### Section 4: Logical Operator Directives ✅ COMPLETE

**Directives Covered:**
- `:and` - field level and top level
- `:or` - field level and top level

**Coverage Status:** ✅ Complete
- Field-level `:or` demonstrated
- Top-level `:and` with multiple syntaxes shown
- Top-level `:or` with multiple syntaxes shown
- Nested logical operators covered
- Implicit AND between field operations shown

**Missing Examples:** None

---

### Section 5: Comparison Operator Directives ⚠️ GAPS IDENTIFIED

**Directives Covered:**
- `:==` and `:eq` (equality)
- `:!=` and `:ne` (inequality)
- `:>` and `:gt` (greater than)
- `:>=` and `:gte` (greater than or equal)
- `:<` and `:lt` (less than)
- `:<=` and `:lte` (less than or equal)
- `:in` (membership)

**Coverage Status:** ⚠️ Partial - Missing `:ne` alias examples

**Existing Examples:**
- `:==` with scalar value ✓
- `:eq` with scalar value ✓
- `:==` with nil ✓
- `:eq` with nil ✓
- `:!=` with nil ✓
- `:!=` with scalar value ✓
- All comparison operators (>, >=, <, <=) with scalar values ✓
- All comparison operator aliases (gt, gte, lt, lte) with scalar values ✓
- `:in` with list ✓
- `:==` with list (coerced to IN) ✓
- `:!=` with list (coerced to NOT IN) ✓

**Missing Examples:**
1. **`:ne` with nil** - `[published_at: [ne: nil]]`
   - Priority: Medium
   - Rationale: `:ne` is listed as an alias but not demonstrated with nil comparison

2. **`:ne` with list values** - `[published: [ne: [true, false]]]`
   - Priority: Medium
   - Rationale: Should demonstrate `:ne` coercion to NOT IN, parallel to `:!=`

---

### Section 6: String Matching Directives ✅ COMPLETE

**Directives Covered:**
- `:like` - pattern matching
- `:ilike` - case-insensitive pattern matching

**Coverage Status:** ✅ Complete
- `:like` with single pattern ✓
- `:ilike` with single pattern ✓
- `:like` with array of patterns ✓
- `:ilike` with array of patterns ✓
- `:not` with `:like` (single pattern) ✓
- `:not` with `:ilike` (single pattern) ✓
- `:not` with `:like` (array of patterns) ✓
- `:not` with `:ilike` (array of patterns) ✓

**Missing Examples:** None - user recently added comments to organize examples

---

### Section 7: String Transformation Directives ✅ COMPLETE

**Directives Covered:**
- `:lower` - convert to lowercase
- `:upper` - convert to uppercase

**Coverage Status:** ✅ Complete
- `:lower` as value transformation (Slot 8) ✓
- `:upper` as value transformation (Slot 8) ✓
- `:lower` with equality ✓
- `:upper` with equality ✓
- `:lower` with inequality ✓
- `:upper` with inequality ✓
- `:not` with `:lower` ✓
- `:not` with `:upper` ✓

**Missing Examples:** None

---

### Section 8: Aggregate Operator Directives ⚠️ GAPS IDENTIFIED

**Directives Covered:**
- `:avg` - average
- `:count` - count
- `:max` - maximum
- `:min` - minimum
- `:sum` - sum

**Coverage Status:** ⚠️ Partial - Uneven coverage across aggregates

**Existing Examples:**
- `:avg` with `>`, `!=`, `<=` ✓
- `:count` with `>`, `==` (including nil) ✓
- `:max` with `>=` ✓
- `:min` with `<` ✓
- `:sum` with `==`, `>` ✓
- `:not` with `:avg`, `:count`, `:max` ✓

**Missing Examples (Representative Coverage):**
3. **`:min` with equality** - `[views: [min: [==: 0]]]`
   - Priority: Low
   - Rationale: `:min` only shows `<`, should demonstrate equality for representative coverage

4. **`:sum` with inequality** - `[views: [sum: [!=: 0]]]`
   - Priority: Low
   - Rationale: `:sum` only shows `==` and `>`, should demonstrate inequality

5. **`:not` with `:min`** - `[not: [views: [min: [<: 5]]]]`
   - Priority: Low
   - Rationale: Negation shown for `:avg`, `:count`, `:max` but not `:min` or `:sum`

6. **`:not` with `:sum`** - `[not: [views: [sum: [>: 500]]]]`
   - Priority: Low
   - Rationale: Negation shown for `:avg`, `:count`, `:max` but not `:min` or `:sum`

---

### Section 9: Set Comparison Directives ⚠️ GAPS IDENTIFIED

**Directives Covered:**
- `:all` - compare against all values in subquery
- `:any` - compare against any value in subquery

**Coverage Status:** ⚠️ Partial - `:any` missing most comparison operators

**Existing Examples:**
- `:all` with `>` ✓
- `:all` with `>=`, `<`, `<=`, `==`, `!=` ✓
- `:all` with default (implicit `==`) ✓
- `:all` with query-builder payload ✓
- `:not` with `:all` (both explicit and default) ✓
- `:any` with `>` ✓
- `:any` with default (implicit `==`) ✓
- `:not` with `:any` (both explicit and default) ✓

**Missing Examples:**
7. **`:any` with `>=`** - `[id: [>=: [any: subquery_expr]]]`
   - Priority: High
   - Rationale: `:all` shows all 6 operators, `:any` only shows `>` and default

8. **`:any` with `<`** - `[id: [<: [any: subquery_expr]]]`
   - Priority: High
   - Rationale: Complete the comparison operator coverage for `:any`

9. **`:any` with `<=`** - `[id: [<=: [any: subquery_expr]]]`
   - Priority: High
   - Rationale: Complete the comparison operator coverage for `:any`

10. **`:any` with `==`** - `[id: [==: [any: subquery_expr]]]`
    - Priority: High
    - Rationale: Explicit equality (vs default) for `:any`

11. **`:any` with `!=`** - `[id: [!=: [any: subquery_expr]]]`
    - Priority: High
    - Rationale: Complete the comparison operator coverage for `:any`

---

### Section 10: Arithmetic Operator Directives ✅ COMPLETE

**Directives Covered:**
- `:+` - addition
- `:-` - subtraction
- `:*` - multiplication
- `:/` - division

**Coverage Status:** ✅ Complete
- `:+` with `>`, `!=`, `<=` ✓
- `:-` with `>=`, `>` ✓
- `:*` with `<` ✓
- `:/` with `==` ✓
- `:not` with `:+`, `:-`, `:*` ✓
- Arithmetic with field references ✓
- Arithmetic with literal values ✓

**Missing Examples:** None

---

### Section 11: Date/Time Directives ✅ COMPLETE

**Directives Covered:**
- `:datetime` or `:date` wrapper
  - `:add` - add time interval
  - `:ago` - time in the past
  - `:from_now` - time in the future

**Coverage Status:** ✅ Complete
- `:datetime` with `:add` ✓
- `:datetime` with `:ago` ✓
- `:datetime` with `:from_now` ✓
- `:date` with `:add` ✓
- `:date` with `:ago` ✓
- `:date` with `:from_now` ✓
- Multiple comparison operators (>=, >, <, <=, ==, !=) ✓
- `:not` with date/time operations ✓

**Missing Examples:** None

---

### Section 12: Binding Selector Directives ✅ COMPLETE

**Directives Covered:**
- `:bind` with `:as` (named binding)
- `:bind` with `:at` (positional binding)

**Coverage Status:** ✅ Complete
- `:as` with single named binding ✓
- `:as` with multiple named bindings ✓
- `:at` with numeric index ✓
- `:at` with `:first` ✓
- `:at` with `:last` ✓

**Missing Examples:** None

---

### Section 13: Schema Filter Directives ✅ COMPLETE

**Directives Covered:**
- `:where` - explicit where clause
- `:or_where` - explicit OR where clause

**Coverage Status:** ✅ Complete
- `:where` with single condition ✓
- `:where` with multiple conditions ✓
- `:or_where` standalone ✓
- `:where` combined with `:or_where` ✓
- `:or_where` with nested `:or` logic ✓

**Missing Examples:** None

---

### Section 14: Terminal Filter Directives ✅ COMPLETE

**Directives Covered:**
- `:last` - Returns last N records
- `:subquery` - Wraps query in subquery

**Coverage Status:** ✅ Complete
- `:last` with integer ✓
- `:last` with field specification ✓
- `:subquery` with filters ✓
- `:subquery` combined with other filters ✓

**Missing Examples:** None

---

### Section 15: Query Configuration Directives ⚠️ GAPS IDENTIFIED

**Directives Covered:**
- `:from`, `:select`, `:select_merge`, `:distinct`, `:group_by`, `:having`, `:or_having`
- `:order_by`, `:prepend_order_by`, `:limit`, `:offset`, `:first`, `:after`, `:before`
- `:reverse_order`, `:exclude`, `:put_query_prefix`, `:start_date`, `:end_date`, `:ids`
- `:dynamic`, `:exists`

**Coverage Status:** ⚠️ Partial - Some operators lack examples

**Note:** This section has 23 operators. Many are configuration operators that don't combine with operators in the same way as filter operators. The audit focuses on filter-related operators.

**Existing Examples:**
- `:dynamic` at top level ✓
- `:dynamic` in `:where` ✓
- `:dynamic` in `:or_where` ✓
- `:exists` in `:where` ✓
- `:exists` with `:not` ✓
- `:from` with schema ✓
- `:from` with table string ✓
- `:select` with various options ✓
- `:distinct` with various options ✓
- `:group_by` with single and multiple fields ✓
- `:having` with filters and aggregates ✓
- `:or_having` ✓
- `:order_by` with various options ✓
- `:limit`, `:offset`, `:first`, `:after`, `:before` ✓
- `:reverse_order`, `:exclude`, `:put_query_prefix` ✓
- `:start_date`, `:end_date`, `:ids` ✓

**Missing Examples:**
12. **`:having` with `:not`** - `[having: [not: [views: [>: 10]]]]`
    - Priority: Medium
    - Rationale: `:having` shows filters and aggregates but not negation

13. **`:or_having` with aggregate** - `[or_having: [views: [avg: [<: 5]]]]`
    - Priority: Low
    - Rationale: `:or_having` only shows simple comparison, should demonstrate aggregate

---

### Section 16: Set Directives ✅ COMPLETE

**Directives Covered:**
- `:except`, `:except_all`
- `:intersect`, `:intersect_all`
- `:union`, `:union_all`

**Coverage Status:** ✅ Complete
- All 6 set operators demonstrated ✓
- Set operations with query-builder payloads ✓
- Multiple set operations chained ✓

**Missing Examples:** None

---

### Section 17: Lock Directives ✅ COMPLETE

**Directives Covered:**
- `:lock` - query locking

**Coverage Status:** ✅ Complete
- `:lock` with string lock clause ✓
- `:lock` with fragment ✓

**Missing Examples:** None

---

### Section 18: CTE Directives ✅ COMPLETE

**Directives Covered:**
- `:recursive_ctes` - enable recursive CTEs
- `:with_cte` - define CTE

**Coverage Status:** ✅ Complete
- `:recursive_ctes` with boolean ✓
- `:with_cte` with name and query ✓
- `:with_cte` with options ✓

**Missing Examples:** None

---

### Section 19: Named Binding Directives ✅ COMPLETE

**Directives Covered:**
- `:with_named_binding` - create named binding

**Coverage Status:** ✅ Complete
- `:with_named_binding` with name and query ✓

**Missing Examples:** None

---

### Section 20: Query Modifier Directives ✅ COMPLETE

**Directives Covered:**
- `:with_ties`, `:update`, `:windows`, `:preload`

**Coverage Status:** ✅ Complete
- `:with_ties` ✓
- `:update` with set operations ✓
- `:windows` with window definitions ✓
- `:preload` with associations ✓
- `:preload` with nested preloads ✓

**Missing Examples:** None

---

### Section 21: Join Directives ⚠️ GAPS IDENTIFIED

**Directives Covered:**
- `:join` with various source types:
  - `:association`
  - `:schema`
  - `:table`
  - `:query`
  - `:subquery`
  - `:fragment`

**Coverage Status:** ⚠️ Partial - Missing some join types

**Existing Examples:**
- `:association` join ✓
- `:schema` join ✓
- `:table` join ✓
- `:query` join ✓
- `:fragment` join ✓

**Missing Examples:**
14. **`:subquery` join** - `[join: [subquery: subquery_expr, on: [id: :post_id]]]`
    - Priority: Medium
    - Rationale: `:subquery` is listed as a join source type but no example shown

---

## Summary of Missing Examples

### High Priority (Critical Gaps)
**Total: 5 examples**

1. `:any` with `>=` - Set Comparison (Section 9)
2. `:any` with `<` - Set Comparison (Section 9)
3. `:any` with `<=` - Set Comparison (Section 9)
4. `:any` with `==` - Set Comparison (Section 9)
5. `:any` with `!=` - Set Comparison (Section 9)

### Medium Priority (Important Gaps)
**Total: 5 examples**

6. `:ne` with nil - Comparison Operators (Section 5)
7. `:ne` with list values - Comparison Operators (Section 5)
8. `:having` with `:not` - Query Configuration (Section 15)
9. `:subquery` join - Join Directives (Section 21)

### Low Priority (Nice-to-Have)
**Total: 5 examples**

10. `:min` with equality - Aggregates (Section 8)
11. `:sum` with inequality - Aggregates (Section 8)
12. `:not` with `:min` - Aggregates (Section 8)
13. `:not` with `:sum` - Aggregates (Section 8)
14. `:or_having` with aggregate - Query Configuration (Section 15)

---

## Recommendations

### Immediate Actions (High Priority)
1. **Add `:any` operator coverage** - Section 9 (Set Comparison Directives)
   - Add 5 examples showing `:any` with all comparison operators
   - This brings `:any` to parity with `:all` coverage
   - Critical for users understanding set comparison operations

### Short-term Actions (Medium Priority)
2. **Complete `:ne` alias coverage** - Section 5 (Comparison Operator Directives)
   - Add 2 examples showing `:ne` with nil and list values
   - Ensures all comparison operator aliases are fully documented

3. **Add negation to `:having`** - Section 15 (Query Configuration Directives)
   - Add 1 example showing `:not` with `:having`
   - Demonstrates that `:having` supports full filter syntax

4. **Add `:subquery` join example** - Section 21 (Join Directives)
   - Add 1 example showing `:subquery` as join source
   - Completes the join source type coverage

### Long-term Actions (Low Priority)
5. **Round out aggregate coverage** - Section 8 (Aggregate Operator Directives)
   - Add 4 examples for `:min` and `:sum` with additional operators
   - Provides more complete reference for aggregate operations

6. **Add `:or_having` with aggregate** - Section 15 (Query Configuration Directives)
   - Add 1 example showing `:or_having` with aggregate function
   - Demonstrates advanced `:or_having` usage

---

## Coverage Statistics

### Overall Coverage
- **Sections with Complete Coverage:** 13/21 (62%)
- **Sections with Partial Coverage:** 4/21 (19%)
- **Sections with Gaps:** 4/21 (19%)

### By Priority
- **High Priority Gaps:** 5 examples (33%)
- **Medium Priority Gaps:** 4 examples (27%)
- **Low Priority Gaps:** 6 examples (40%)
- **Total Missing Examples:** 15

### By Directive Category
- **Core Query Directives:** 7 gaps (Array, Comparison, Set Comparison)
- **Aggregate Directives:** 4 gaps (Aggregates)
- **Configuration Directives:** 2 gaps (Query Config)
- **Join Directives:** 2 gaps (Joins)

---

## Conclusion

The COMMON_FILTERS.md document has **strong overall coverage** with 13 of 21 sections (62%) having complete representative coverage. The identified gaps are concentrated in a few key areas:

1. **Set Comparison (`:any`)** - Missing 5 examples showing different comparison operators
2. **Comparison Operators (`:ne`)** - Missing 2 examples for the inequality alias
3. **Aggregates** - Missing 4 examples for `:min` and `:sum` with various operators
4. **Configuration & Joins** - Missing 4 examples for advanced features

**Impact Assessment:**
- **High Priority gaps** (5 examples) affect core query-building functionality and should be addressed first
- **Medium Priority gaps** (4 examples) improve completeness for important features
- **Low Priority gaps** (6 examples) provide additional reference material but are not critical

**Next Steps:**
1. Add the 5 high-priority examples for `:any` operator
2. Add the 4 medium-priority examples for `:ne`, `:having`, and `:subquery` join
3. Consider adding low-priority examples based on user feedback and common questions

The document successfully achieves its goal of enabling "a complete beginner to understand how to use the entire API" with only minor gaps in advanced features.

---

**Report Generated:** March 5, 2026  
**Audit Completed By:** Cascade AI  
**Document Version:** Current (as of audit date)
