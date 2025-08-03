use actix::prelude::*;
use actix_cors::Cors;
use actix_files as fs;
use actix_web::{
    get, post,
    web::{self, Data, Json, Path, Payload},
    App, HttpRequest, HttpResponse, HttpServer, Responder,
};
use actix_web_actors::ws;
use futures::executor::block_on;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
    time::Duration,
};
use tokio::{sync::Mutex, task, time::sleep};
use uuid::Uuid;

mod tasks;
use tasks::*;

type Sessions = Arc<Mutex<HashMap<Uuid, Addr<MyWs>>>>;

// WebSocket Actor

#[derive(Message)]
#[rtype(result = "()")]
struct WsMessage(pub String);

struct MyWs {
    id: Uuid,
    sessions: Sessions,
}

impl Actor for MyWs {
    type Context = ws::WebsocketContext<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        let addr = ctx.address();
        block_on(self.sessions.lock()).insert(self.id, addr);
    }
    fn stopped(&mut self, _ctx: &mut Self::Context) {
        block_on(self.sessions.lock()).remove(&self.id);
    }
}

impl Handler<WsMessage> for MyWs {
    type Result = ();
    fn handle(&mut self, msg: WsMessage, ctx: &mut Self::Context) {
        ctx.text(msg.0);
    }
}

impl StreamHandler<Result<ws::Message, ws::ProtocolError>> for MyWs {
    fn handle(&mut self, _: Result<ws::Message, ws::ProtocolError>, _: &mut Self::Context) {}
}

// 任务队列

#[derive(Clone)]
struct Job {
    id: Uuid,
    module: String,
    version: Option<String>,
}
#[derive(Default)]
struct QueueState {
    running: bool,
    queue: VecDeque<Job>,
}
type Queue = Arc<Mutex<QueueState>>;

// DTO

#[derive(Deserialize)]
struct SubmitRequest {
    module: String,
    version: Option<String>,
}

#[derive(Serialize)]
struct SubmitResp {
    id: Uuid,
}

#[derive(Deserialize)]
struct FeedbackReq {
    id: Uuid,
    ack: String,
}

// 路由

#[post("/submit")]
async fn submit(
    sessions: Data<Sessions>,
    queue: Data<Queue>,
    Json(req): Json<SubmitRequest>,
) -> impl Responder {
    let id = Uuid::new_v4();
    let job = Job {
        id,
        module: req.module,
        version: req.version,
    };

    {
        let mut q = queue.lock().await;
        q.queue.push_back(job);
        if !q.running {
            q.running = true;
            spawn_worker(queue.clone(), sessions.clone());
        }
    }

    HttpResponse::Ok().json(SubmitResp { id })
}

#[get("/ws/{id}")]
async fn ws_index(
    req: HttpRequest,
    stream: Payload,
    path: Path<Uuid>,
    sessions: Data<Sessions>,
) -> actix_web::Result<HttpResponse> {
    let id = path.into_inner();
    ws::start(
        MyWs {
            id,
            sessions: sessions.get_ref().clone(),
        },
        &req,
        stream,
    )
}

#[post("/feedback")]
async fn feedback(Json(req): Json<FeedbackReq>) -> impl Responder {
    println!("ACK {}: {}", req.id, req.ack);
    HttpResponse::Ok().body("received")
}

// worker

fn spawn_worker(queue: Data<Queue>, sessions: Data<Sessions>) {
    task::spawn(async move {
        loop {
            // 1. 取队第一个任务
            let job = {
                let mut q = queue.lock().await;
                q.queue.pop_front()
            };

            // 队列空则标记停止 & 退出
            let job = match job {
                Some(j) => j,
                None => {
                    queue.lock().await.running = false;
                    break;
                }
            };

            // 2. 按 module 调度
            let exec_result = match job.module.as_str() {
                "classify_versions" => classify_versions_task().await,
                "original_download" => {
                    let current_path = std::env::current_dir().expect("Failed to get current directory");
                    let mc_home = current_path.join(".minecraft");
                    // 如果前端没带版本就用默认
                    let ver = job.version.as_deref().unwrap_or("1.20.4");
                    original_download_task(ver, &mc_home).await
                }
                unknown => Ok(format!("Unknown module: {unknown}")),
            };

            // 3. 结果文本化
            let result_text = match exec_result {
                Ok(s)  => s,
                Err(e) => format!("Task failed: {e}"),
            };

            // 4. 推送到对应 WebSocket 会话
            if let Some(addr) = sessions.lock().await.get(&job.id) {
                addr.do_send(WsMessage(result_text));
            }
        }
    });
}


// main

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let sessions: Sessions = Arc::new(Mutex::new(HashMap::new()));
    let queue: Queue = Arc::new(Mutex::new(QueueState::default()));

    HttpServer::new(move || {
        App::new()
            .wrap(
                Cors::default()
                    .allow_any_origin()
                    .allow_any_header()
                    .allow_any_method()
                    .max_age(3600),
            )
            .app_data(Data::new(sessions.clone()))
            .app_data(Data::new(queue.clone()))
            .service(submit)
            .service(ws_index)
            .service(feedback)
            .service(fs::Files::new("/", "./").index_file("index.html"))
    })
        .bind(("0.0.0.0", 3000))?
        .run()
        .await
}
