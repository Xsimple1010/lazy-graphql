use crate::{auth::token::Token, service_state::ServiceState};
use async_graphql::{Data, ObjectType, Result, SubscriptionType};
use async_graphql_axum::{GraphQLProtocol, GraphQLWebSocket};
use axum::{
    extract::{State, WebSocketUpgrade},
    response::IntoResponse,
};
use serde::{Deserialize, de::DeserializeOwned};

#[derive(Deserialize)]
struct Payload {
    #[serde(alias = "AUTHORIZATION", alias = "Authorization")]
    authorization: String,
}

pub async fn on_connection_init<
    Query: ObjectType + 'static,
    Mutation: ObjectType + 'static,
    Subscription: SubscriptionType + 'static,
    ActorClaims: DeserializeOwned + Send + Sync + 'static,
    S: ServiceState<Query, Mutation, Subscription, ActorClaims> + 'static,
>(
    value: serde_json::Value,
    mut state: S,
) -> Result<Data> {
    if let Ok(_payload) = serde_json::from_value::<Payload>(value) {
        println!("tenta recuperar o token");
        let claims = Token::validade_as_firebase(&_payload.authorization).await?;
        state.set_claims(Some(claims));
    }

    let mut data = Data::default();
    data.insert(state);
    Ok(data)
}

pub async fn graphql_subscription_operation<
    Query: ObjectType + 'static,
    Mutation: ObjectType + 'static,
    Subscription: SubscriptionType + 'static,
    S: ServiceState<Query, Mutation, Subscription, ActorClaims> + Clone + 'static,
    ActorClaims: DeserializeOwned + Send + Sync + 'static,
>(
    State(state): State<S>,
    protocol: GraphQLProtocol,
    ws: WebSocketUpgrade,
) -> impl IntoResponse {
    let schema = state.get_graphql_schema().clone();

    ws.protocols(["graphql-transport-ws", "graphql-ws"])
        .on_upgrade(move |socket| {
            GraphQLWebSocket::new(socket, schema, protocol)
                .on_connection_init(move |value| {
                    // Clona o state para poder mutar dentro do closure
                    let state = state.clone();
                    async move { on_connection_init(value, state).await }
                })
                .serve()
        })
}
