use crate::service_state::ServiceState;
use async_graphql::SubscriptionType;
use serde::de::DeserializeOwned;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use tokio_cron_scheduler::{Job, JobScheduler};

pub type JobHandler = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

pub struct SubgraphJob<
    Query: async_graphql::ObjectType + 'static,
    Mutation: async_graphql::ObjectType + 'static,
    Subscription: SubscriptionType + 'static,
    S: ServiceState<Query, Mutation, Subscription, ActorClaims>,
    ActorClaims: DeserializeOwned + Send + Sync + 'static,
> {
    pub name: &'static str,
    pub schedule: &'static str,
    pub handler: fn(S) -> JobHandler,
    pub _phantom: PhantomData<(Query, Mutation, Subscription, ActorClaims)>,
}

pub async fn start_jobs<
    Query: async_graphql::ObjectType + 'static,
    Mutation: async_graphql::ObjectType + 'static,
    Subscription: SubscriptionType + 'static,
    S: ServiceState<Query, Mutation, Subscription, ActorClaims> + 'static,
    ActorClaims: DeserializeOwned + Send + Sync + 'static,
>(
    jobs: Vec<SubgraphJob<Query, Mutation, Subscription, S, ActorClaims>>,
    state: S,
) {
    if jobs.is_empty() {
        return;
    }
    let scheduler = JobScheduler::new().await.unwrap();
    for job in jobs {
        let state = state.clone();
        let job_schedule = Job::new_async(job.schedule, move |_, _| (job.handler)(state.clone()))
            .unwrap_or_else(|_| panic!("Erro ao criar job cron: {}", job.name));
        scheduler.add(job_schedule).await.unwrap();
    }
    scheduler.start().await.unwrap();
}
