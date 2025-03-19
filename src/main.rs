use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde::de::DeserializeOwned;
use sqlx::postgres::PgPool;
use sqlx::Postgres;
use uuid::Uuid;
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
        id: Uuid::new_v4(),
        version: 3i16,
        created_at: Utc::now(),
        payload: address
    }; 

    let event2: ESEvent<Addres> = ESEvent {
        id: Uuid::new_v4(),
        version: 4i16,
        created_at: Utc::now(),
        payload: address2
    }; 

    let url = env::var("DATABASE_URL").unwrap();
    let pool_future = PgPool::connect(&url);
    let pool = block_on(pool_future).unwrap();



    let event_store: PgEventStore = PgEventStore {
        pool
    };


    let binding = vec![event, event2];
    let future = event_store.start_esstream(&binding, 0);

    let res = block_on(future);
    match res {
        Ok(_) => {
            println!("Added the events to db!");
        },
        Err(err) => {
            println!("{}", &err);
        },
    }

    let query_stream = event_store.get_esstream(0);
    let future: Result<ESStream<Addres>, &'static str> = block_on(query_stream);

    match future {
        Ok(data) => {
            println!("Added the events to db!, {:?}", data);
        },
        Err(err) => {
            print!("{}", err)
        },
    }

}

#[derive(Serialize, Deserialize, Debug)]
struct Addres {
    street: String,
    city: String,
}

#[derive(Debug)]
struct ESEvent<T: Serialize + DeserializeOwned> {
    id: Uuid,
    version: i16,
    created_at: DateTime<Utc>,
    payload: T
}

#[derive(Debug)]
struct ESStream<T: Serialize + DeserializeOwned> {
    id: i64,
    events: Vec<ESEvent<T>>,
}

// impl<T: Serialize + DeserializeOwned> ESStream<T> {
//     fn get_timestamp(&self) -> DateTime<Utc> {
//         *self.events
//             .first()
//             .map(|a| a.timestamp)
//             .get_or_insert(0i32)
//     }

//     fn get_latest_version(&self) -> i32 {
//         *self.events
//             .last()
//             .map(|a| a.version)
//             .get_or_insert(0i32)
//     }
// }

trait EventStoreOps<T: Serialize + DeserializeOwned> {
    async fn start_esstream(&self, first_events: &Vec<ESEvent<T>>, stream_id: i64) -> Result<(), &'static str>;
    // async fn add_esevents_to_esstream(estream: &ESStream<T>, new_events: &Vec<ESEvent<T>>) -> Result<(), &'static str>;
    async fn get_esstream(&self, stream_id:i64) -> Result<ESStream<T>, &'static str>;
}

struct PgEventStore {
    pool: sqlx::Pool<Postgres>
}

impl<T: Serialize + DeserializeOwned> EventStoreOps<T> for PgEventStore {
    
    async fn start_esstream(&self, first_events: &Vec<ESEvent<T>>, stream_id: i64) -> Result<(), &'static str> {

        let versions: Vec<i16> = first_events.iter().map(|e| e.version.clone()).collect();
        let payloads: Vec<String> = first_events.iter().map(|e| serde_json::to_string(&e.payload).unwrap()).collect();
        let stream_ids: Vec<i64> = first_events.iter().map(|e| stream_id).collect();
        let event_ids: Vec<Uuid> = first_events.iter().map(|e| e.id).collect();
        let timestaps: Vec<DateTime<Utc>> = first_events.iter().map(|e| e.created_at).collect();


        let rec2 = sqlx::query!(r#"INSERT INTO events (stream_id, version, payload, event_id, created_at) 
            SELECT * from UNNEST ($1::BIGINT[], $2::SMALLINT[], $3::TEXT[], $4::UUID[], $5::TIMESTAMPTZ[])
        "#, &stream_ids, &versions, &payloads, &event_ids, &timestaps).execute(&self.pool).await;

        match rec2 {
            Ok(_) => Ok(()),
            Err(_) => Err("could not insert values")
        }
    }
    
    async fn get_esstream(&self, stream_id:i64) -> Result<ESStream<T>, &'static str> {
        let recs = sqlx::query!(r#"SELECT stream_id, version, payload, event_id, created_at FROM events WHERE stream_id = $1"#, stream_id).fetch_all(&self.pool).await;

        match recs {
            Ok(data) => {
                let events = data.iter().map(|d| ESEvent{
                    version : d.version,
                    id: d.event_id.unwrap(),
                    created_at: d.created_at.unwrap(),
                    payload: serde_json::from_str(&d.payload.clone().unwrap()).unwrap()
                }).collect();

                let stream = ESStream {
                   events,
                   id: stream_id
                };

                Ok(stream)

            },
            Err(_) => Err("could not fetch"),
        }


    }

}