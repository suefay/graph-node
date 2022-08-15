use crate::prelude::q;
use graphql_parser::query::{
    Definition, Directive, Document, Field, FragmentDefinition, Mutation, Number,
    OperationDefinition, Query, Selection, SelectionSet, Subscription, Text, Value,
    VariableDefinition,
};
use graphql_tools::ast::{OperationTransformer, Transformed, TransformedValue};
use std::cmp::Ordering;
use std::collections::{hash_map::DefaultHasher, BTreeMap};
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;

pub fn query_hash(query: &q::Document) -> u64 {
    let mut query_normalization = QueryNormalization::new();
    let normalized_query = query_normalization
        .transform_document(&query)
        .replace_or_else(|| query.clone());

    let mut hasher = DefaultHasher::new();
    normalized_query.to_string().hash(&mut hasher);
    hasher.finish()
}

pub struct QueryNormalization<'a, T: Text<'a> + Clone> {
    phantom: PhantomData<&'a T>,
}

impl<'a, T: Text<'a> + Clone> QueryNormalization<'a, T> {
    pub fn new() -> Self {
        Self {
            phantom: PhantomData,
        }
    }
}

impl<'a, T: Text<'a> + Clone> OperationTransformer<'a, T> for QueryNormalization<'a, T> {
    fn transform_document(
        &mut self,
        document: &Document<'a, T>,
    ) -> TransformedValue<Document<'a, T>> {
        let mut next_definitions = self
            .transform_list(&document.definitions, Self::transform_definition)
            .replace_or_else(|| document.definitions.to_vec());
        next_definitions.sort_unstable_by(|a, b| self.compare_definitions(a, b));
        TransformedValue::Replace(Document {
            definitions: next_definitions,
        })
    }

    fn transform_operation(
        &mut self,
        operation: &OperationDefinition<'a, T>,
    ) -> Transformed<OperationDefinition<'a, T>> {
        match operation {
            OperationDefinition::Query(query) => {
                let selections = self.transform_selection_set(&query.selection_set);
                let directives = self.transform_directives(&query.directives);
                let variable_definitions =
                    self.transform_variable_definitions(&query.variable_definitions);

                // Replace the original query with the transformed one.
                Transformed::Replace(OperationDefinition::Query(Query {
                    directives: directives.replace_or_else(|| query.directives.clone()),
                    selection_set: SelectionSet {
                        items: selections.replace_or_else(|| query.selection_set.items.clone()),
                        span: query.selection_set.span,
                    },
                    variable_definitions: variable_definitions
                        .replace_or_else(|| query.variable_definitions.clone()),
                    position: query.position,
                    // Use a single name for all the queries
                    name: Some("normalized".into()),
                }))
            }
            OperationDefinition::Mutation(mutation) => {
                let selections = self.transform_selection_set(&mutation.selection_set);
                let directives = self.transform_directives(&mutation.directives);
                let variable_definitions =
                    self.transform_variable_definitions(&mutation.variable_definitions);

                // Replace the original mutation with the transformed one.
                Transformed::Replace(OperationDefinition::Mutation(Mutation {
                    directives: directives.replace_or_else(|| mutation.directives.clone()),
                    selection_set: SelectionSet {
                        items: selections.replace_or_else(|| mutation.selection_set.items.clone()),
                        span: mutation.selection_set.span,
                    },
                    variable_definitions: variable_definitions
                        .replace_or_else(|| mutation.variable_definitions.clone()),
                    position: mutation.position,
                    // Use a single name for all the mutations
                    name: Some("normalized".into()),
                }))
            }
            OperationDefinition::Subscription(subscription) => {
                let selections = self.transform_selection_set(&subscription.selection_set);
                let directives = self.transform_directives(&subscription.directives);
                let variable_definitions =
                    self.transform_variable_definitions(&subscription.variable_definitions);

                // Replace the original subscription with the transformed one.
                Transformed::Replace(OperationDefinition::Subscription(Subscription {
                    directives: directives.replace_or_else(|| subscription.directives.clone()),
                    selection_set: SelectionSet {
                        items: selections
                            .replace_or_else(|| subscription.selection_set.items.clone()),
                        span: subscription.selection_set.span,
                    },
                    variable_definitions: variable_definitions
                        .replace_or_else(|| subscription.variable_definitions.clone()),
                    position: subscription.position,
                    // Use a single name for all the subscriptions
                    name: Some("normalized".into()),
                }))
            }
            OperationDefinition::SelectionSet(selection_set) => {
                let items = self.transform_selection_set(selection_set);
                // Transform the selection set into a named query
                Transformed::Replace(OperationDefinition::Query(Query {
                    directives: vec![],
                    selection_set: SelectionSet {
                        items: items.replace_or_else(|| selection_set.items.clone()),
                        span: selection_set.span,
                    },
                    variable_definitions: vec![],
                    position: selection_set.span.0,
                    // Match the name with the rest of queries
                    name: Some("normalized".into()),
                }))
            }
        }
    }

    // Transform the primitive values into their default values.
    //  Float: 0.0
    //  Int: 0
    //  String: ""
    fn transform_value(&mut self, node: &Value<'a, T>) -> TransformedValue<Value<'a, T>> {
        match node {
            Value::Float(_) => TransformedValue::Replace(Value::Float(0.0)),
            Value::Int(_) => TransformedValue::Replace(Value::Int(Number::from(0))),
            Value::String(_) => TransformedValue::Replace(Value::String(String::from(""))),
            Value::Variable(_) => TransformedValue::Keep,
            Value::Boolean(_) => TransformedValue::Keep,
            Value::Null => TransformedValue::Keep,
            Value::Enum(_) => TransformedValue::Keep,
            Value::List(val) => {
                let items: Vec<Value<'a, T>> = val
                    .iter()
                    .map(|item| self.transform_value(item).replace_or_else(|| item.clone()))
                    .collect();

                TransformedValue::Replace(Value::List(items))
            }
            Value::Object(fields) => {
                let fields: BTreeMap<T::Value, Value<'a, T>> = fields
                    .iter()
                    .map(|field| {
                        let (name, value) = field;
                        let new_value = self
                            .transform_value(value)
                            .replace_or_else(|| value.clone());
                        (name.clone(), new_value)
                    })
                    .collect();

                TransformedValue::Replace(Value::Object(fields))
            }
        }
    }

    fn transform_field(&mut self, field: &Field<'a, T>) -> Transformed<Selection<'a, T>> {
        let selection_set = self.transform_selection_set(&field.selection_set);
        let arguments = self.transform_arguments(&field.arguments);
        let directives = self.transform_directives(&field.directives);

        Transformed::Replace(Selection::Field(Field {
            arguments: arguments.replace_or_else(|| field.arguments.clone()),
            directives: directives.replace_or_else(|| field.directives.clone()),
            selection_set: SelectionSet {
                items: selection_set.replace_or_else(|| field.selection_set.items.clone()),
                span: field.selection_set.span,
            },
            position: field.position,
            // Remove an alias
            alias: None,
            name: field.name.clone(),
        }))
    }

    // Sort the selection set
    fn transform_selection_set(
        &mut self,
        selections: &SelectionSet<'a, T>,
    ) -> TransformedValue<Vec<Selection<'a, T>>> {
        let mut next_selections = self
            .transform_list(&selections.items, Self::transform_selection)
            .replace_or_else(|| selections.items.to_vec());
        next_selections.sort_unstable_by(|a, b| self.compare_selections(a, b));
        TransformedValue::Replace(next_selections)
    }

    // Sort directives
    fn transform_directives(
        &mut self,
        directives: &Vec<Directive<'a, T>>,
    ) -> TransformedValue<Vec<Directive<'a, T>>> {
        let mut next_directives = self
            .transform_list(&directives, Self::transform_directive)
            .replace_or_else(|| directives.to_vec());
        next_directives.sort_unstable_by(|a, b| self.compare_directives(a, b));
        TransformedValue::Replace(next_directives)
    }

    // Sort arguments
    fn transform_arguments(
        &mut self,
        arguments: &[(T::Value, Value<'a, T>)],
    ) -> TransformedValue<Vec<(T::Value, Value<'a, T>)>> {
        let mut next_arguments = self
            .transform_list(&arguments, Self::transform_argument)
            .replace_or_else(|| arguments.to_vec());
        next_arguments.sort_unstable_by(|a, b| self.compare_arguments(a, b));
        TransformedValue::Replace(next_arguments)
    }

    // Sort variable definitions
    fn transform_variable_definitions(
        &mut self,
        variable_definitions: &Vec<VariableDefinition<'a, T>>,
    ) -> TransformedValue<Vec<VariableDefinition<'a, T>>> {
        let mut next_variable_definitions = self
            .transform_list(&variable_definitions, Self::transform_variable_definition)
            .replace_or_else(|| variable_definitions.to_vec());
        next_variable_definitions.sort_unstable_by(|a, b| self.compare_variable_definitions(a, b));
        TransformedValue::Replace(next_variable_definitions)
    }

    fn transform_fragment(
        &mut self,
        fragment: &FragmentDefinition<'a, T>,
    ) -> Transformed<FragmentDefinition<'a, T>> {
        let mut directives = fragment.directives.clone();
        directives.sort_unstable_by_key(|var| var.name.clone());

        let selections = self.transform_selection_set(&fragment.selection_set);

        Transformed::Replace(FragmentDefinition {
            selection_set: SelectionSet {
                items: selections.replace_or_else(|| fragment.selection_set.items.clone()),
                span: fragment.selection_set.span.clone(),
            },
            directives,
            name: fragment.name.clone(),
            position: fragment.position.clone(),
            type_condition: fragment.type_condition.clone(),
        })
    }

    fn transform_selection(
        &mut self,
        selection: &Selection<'a, T>,
    ) -> Transformed<Selection<'a, T>> {
        match selection {
            Selection::InlineFragment(selection) => self.transform_inline_fragment(selection),
            Selection::Field(field) => self.transform_field(field),
            Selection::FragmentSpread(_) => Transformed::Keep,
        }
    }
}

impl<'a, T: Text<'a> + Clone> QueryNormalization<'a, T> {
    fn compare_definitions(&self, a: &Definition<'a, T>, b: &Definition<'a, T>) -> Ordering {
        match (a, b) {
            // Keep operations as they are
            (Definition::Operation(a), Definition::Operation(b)) => Ordering::Equal,
            // Sort fragments by name
            (Definition::Fragment(a), Definition::Fragment(b)) => a.name.cmp(&b.name),
            // Operation -> Fragment
            _ => definition_kind_ordering(a).cmp(&definition_kind_ordering(b)),
        }
    }

    fn compare_selections(&self, a: &Selection<'a, T>, b: &Selection<'a, T>) -> Ordering {
        match (a, b) {
            (Selection::Field(a), Selection::Field(b)) => a.name.cmp(&b.name),
            (Selection::FragmentSpread(a), Selection::FragmentSpread(b)) => {
                a.fragment_name.cmp(&b.fragment_name)
            }
            _ => {
                let a_ordering = selection_kind_ordering(a);
                let b_ordering = selection_kind_ordering(b);
                a_ordering.cmp(&b_ordering)
            }
        }
    }

    fn compare_directives(&self, a: &Directive<'a, T>, b: &Directive<'a, T>) -> Ordering {
        a.name.cmp(&b.name)
    }

    fn compare_arguments(
        &self,
        a: &(T::Value, Value<'a, T>),
        b: &(T::Value, Value<'a, T>),
    ) -> Ordering {
        a.0.cmp(&b.0)
    }
    fn compare_variable_definitions(
        &self,
        a: &VariableDefinition<'a, T>,
        b: &VariableDefinition<'a, T>,
    ) -> Ordering {
        a.name.cmp(&b.name)
    }
}

/// Assigns an order to different variants of Selection
fn selection_kind_ordering<'a, T: Text<'a>>(selection: &Selection<'a, T>) -> u8 {
    match selection {
        Selection::FragmentSpread(_) => 1,
        Selection::InlineFragment(_) => 2,
        Selection::Field(_) => 3,
    }
}

/// Assigns an order to different variants of Definition
fn definition_kind_ordering<'a, T: Text<'a>>(definition: &Definition<'a, T>) -> u8 {
    match definition {
        Definition::Operation(_) => 1,
        Definition::Fragment(_) => 2,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use graphql_parser::parse_query;

    #[test]
    fn sorted() {
        const Q1: &str = "{ things(first: 10, limit: 20) { e { ... on E { e1 e3 e2 } } f { ... F } a c b d } } fragment F on F { f1 f3 f2 }";
        const Q2: &str = "{ things(limit: 20, first: 10) { a c b d f { ...F } e { ... on E { e3 e1 e2 } } } } fragment F on F { f2 f3 f1 }";
        let q1 = parse_query(Q1)
            .expect("q1 is syntactically valid")
            .into_static();
        let q2 = parse_query(Q2)
            .expect("q2 is syntactically valid")
            .into_static();

        assert_eq!(query_hash(&q1), query_hash(&q2));
    }

    #[test]
    fn do_not_sort_inline_fragments() {
        const Q1: &str = "{ things { ... on ThingsA { a } ... on ThingsB { b } } }";
        const Q2: &str = "{ things { ... on ThingsB { b } ... on ThingsA { a } } }";
        let q1 = parse_query(Q1)
            .expect("q1 is syntactically valid")
            .into_static();
        let q2 = parse_query(Q2)
            .expect("q2 is syntactically valid")
            .into_static();

        assert_ne!(query_hash(&q1), query_hash(&q2));
    }

    #[test]
    fn sort_fragment_spreads() {
        const Q1: &str =
            "{ things { ...A ...B } } fragment A on ThingsA { a } fragment B on ThingsB { b }";
        const Q2: &str =
            "{ things { ...B ...A } } fragment A on ThingsA { a } fragment B on ThingsB { b }";
        let q1 = parse_query(Q1)
            .expect("q1 is syntactically valid")
            .into_static();
        let q2 = parse_query(Q2)
            .expect("q2 is syntactically valid")
            .into_static();

        assert_eq!(query_hash(&q1), query_hash(&q2));
    }

    #[test]
    fn sort_fragment_definitions() {
        const Q1: &str =
            "{ things { ...A ...B } } fragment B on ThingsB { b } fragment A on ThingsA { a } ";
        const Q2: &str =
            "{ things { ...A ...B } } fragment A on ThingsA { a } fragment B on ThingsB { b }";
        let q1 = parse_query(Q1)
            .expect("q1 is syntactically valid")
            .into_static();
        let q2 = parse_query(Q2)
            .expect("q2 is syntactically valid")
            .into_static();

        assert_eq!(query_hash(&q1), query_hash(&q2));
    }

    #[test]
    fn literals_and_operation_name() {
        const Q1: &str = "query a { things(where: { stuff_gt: 20 }) { a c b d } }";
        const Q2: &str = "query b { things(where: { stuff_gt: 30 }) { a c b d } }";
        let q1 = parse_query(Q1)
            .expect("q1 is syntactically valid")
            .into_static();
        let q2 = parse_query(Q2)
            .expect("q2 is syntactically valid")
            .into_static();

        assert_eq!(query_hash(&q1), query_hash(&q2));
    }

    #[test]
    fn selection_set_into_query() {
        const Q1: &str = "query a { things { a c b d } }";
        const Q2: &str = "        { things { a c b d } }";
        let q1 = parse_query(Q1)
            .expect("q1 is syntactically valid")
            .into_static();
        let q2 = parse_query(Q2)
            .expect("q2 is syntactically valid")
            .into_static();

        assert_eq!(query_hash(&q1), query_hash(&q2));
    }

    #[test]
    fn extra_field() {
        const Q1: &str = "{ things { a c b d e } }";
        const Q2: &str = "{ things { a c b d   } }";
        let q1 = parse_query(Q1)
            .expect("q1 is syntactically valid")
            .into_static();
        let q2 = parse_query(Q2)
            .expect("q2 is syntactically valid")
            .into_static();

        assert_ne!(query_hash(&q1), query_hash(&q2));
    }

    #[test]
    fn aliases() {
        const Q1: &str = "{ things {    a c b d e } }";
        const Q2: &str = "{ things { aa:a c b d e } }";
        let q1 = parse_query(Q1)
            .expect("q1 is syntactically valid")
            .into_static();
        let q2 = parse_query(Q2)
            .expect("q2 is syntactically valid")
            .into_static();

        assert_eq!(query_hash(&q1), query_hash(&q2));
    }
}
