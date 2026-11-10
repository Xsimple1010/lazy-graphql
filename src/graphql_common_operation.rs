use async_graphql_axum::{GraphQLRequest, GraphQLResponse};
use axum::extract::State;
use serde::de::DeserializeOwned;

use crate::{auth::token::BearerToken, service_state::ServiceState};

pub async fn graphql_common_operation<
    Query: async_graphql::ObjectType + 'static,
    Mutation: async_graphql::ObjectType + 'static,
    Subscription: async_graphql::SubscriptionType + 'static,
    St: ServiceState<Query, Mutation, Subscription, ActorClaims> + Clone + 'static,
    ActorClaims: DeserializeOwned + Send + Sync + 'static,
>(
    State(mut state): State<St>,
    BearerToken(claims): BearerToken<ActorClaims>,
    req: GraphQLRequest, // ← this is the correct extractor
) -> GraphQLResponse {
    state.set_claims(claims);

    // Convert to the inner async_graphql::Request
    let gql_req = req.into_inner().data(state.clone());

    // If you need to inject data into the request context:
    // let gql_req = gql_req.data(claims); // or whatever you need

    state.execute_common_op_with_schema(gql_req).await.into() // converts Response → GraphQLResponse
}
