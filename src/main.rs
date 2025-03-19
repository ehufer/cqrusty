use serde::{Deserialize, Serialize};
use serde::de::DeserializeOwned;
use sqlx::postgres::PgPool;
use std::env;
use futures::executor::block_on;

fn main()  {

    let address = Addres {
        street: "10 Downing Street".to_owned(),
        city: "London".to_owned(),
    };

    let address2 = Addres {
        street: "10 Uping Street".to_owned(),
        city: "Puyallup".to_owned(),
    };

    let event: ESEvent<Addres> = ESEvent {
        version : 3i32,
        timestamp : 1i32,
        type_name: "Addres".to_owned(),
        payload: address
    }; 

    let event2: ESEvent<Addres> = ESEvent {
        version : 4i32,
        timestamp : 1i32,
        type_name: "Addres".to_owned(),
        payload: address2
    }; 

    let eventStore: PgEventStore = PgEventStore {};


    let binding = vec![event, event2];
    let future = eventStore.start_esstream(&binding, 0);
    block_on(future);

    println!("Added the events to db!");
}

struct ESEvent<T: Serialize + DeserializeOwned> {
    version: i32,
    timestamp: i32,
    type_name: String,
    payload: T
}

#[derive(Serialize, Deserialize)]
struct Addres {
    street: String,
    city: String,
}

struct OutboxEvent<T: Serialize + DeserializeOwned> {
    timestamp: u64,
    type_name: String,
    payload: T
}

struct ESStream<T: Serialize + DeserializeOwned> {
    id: u64,
    events: Vec<ESEvent<T>>,
}

impl<T: Serialize + DeserializeOwned> ESStream<T> {
    fn get_timestamp(&self) -> i32 {
        *self.events
            .first()
            .map(|a| a.timestamp)
            .get_or_insert(0i32)
    }

    fn get_latest_version(&self) -> i32 {
        *self.events
            .last()
            .map(|a| a.version)
            .get_or_insert(0i32)
    }
}

trait EventStoreOps<T: Serialize + DeserializeOwned> {
    async fn start_esstream(&self, first_events: &Vec<ESEvent<T>>, stream_id: i32) -> Result<(), &'static str>;
    // fn add_esevents_to_esstream(estream: &ESStream<T>, new_events: &Vec<ESEvent<T>>) -> impl Future<Output = ()>;
    // fn get_esstream(id:u64) -> impl Future<Output = ESStream<T>>;
}

struct PgEventStore {
}

impl<T: Serialize + DeserializeOwned> EventStoreOps<T> for PgEventStore {
    
    async fn start_esstream(&self, first_events: &Vec<ESEvent<T>>, stream_id: i32) -> Result<(), &'static str> {

        let versions: Vec<i32> = first_events.iter().map(|e| e.version.clone()).collect();
        let payloads: Vec<String> = first_events.iter().map(|e| serde_json::to_string(&e.payload).unwrap()).collect();
        let stream_ids: Vec<i32> = first_events.iter().map(|e| stream_id).collect();
        let timestaps: Vec<i32> = first_events.iter().map(|e| e.timestamp).collect();

        let pool = PgPool::connect(&env::var("DATABASE_URL").unwrap()).await.unwrap();

        let rec2 = sqlx::query!(r#"INSERT INTO events (id, stream_id, payload, time_stamp) 
            SELECT * from UNNEST ($1::INT[], $2::INT[], $3::TEXT[], $4::INT[])
        "#, &versions, &stream_ids, &payloads, &timestaps ).execute(&pool).await;

        Ok(())
    }

}