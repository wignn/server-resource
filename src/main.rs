use axum::{
    extract::ws::{Message, WebSocket, WebSocketUpgrade},
    response::IntoResponse,
    routing::get,
    Router,
};
use serde::Serialize;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::time::sleep;

#[derive(Serialize, Clone)]
struct CpuStats {
    model: String,
    cores: usize,
    #[serde(rename = "usagePercent")]
    usage_percent: f32,
}

#[derive(Serialize, Clone)]
struct MemoryStats {
    total: String,
    used: String,
    #[serde(rename = "usagePercent")]
    usage_percent: f32,
}

#[derive(Serialize, Clone)]
struct DiskStats {
    total: String,
    used: String,
    #[serde(rename = "usagePercent")]
    usage_percent: f32,
}

#[derive(Serialize, Clone)]
struct NetworkStats {
    rx: String,
    tx: String,
}

#[derive(Serialize, Clone)]
struct SystemStats {
    hostname: String,
    uptime: String,
    #[serde(rename = "uptimeSeconds")]
    uptime_seconds: u64,
    cpu: CpuStats,
    memory: MemoryStats,
    disk: DiskStats,
    network: NetworkStats,
    os: String,
    processes: usize,
    #[serde(rename = "loadAverage")]
    load_average: String,
    status: String,
    #[serde(rename = "lastUpdated")]
    last_updated: String,
    #[serde(rename = "cpuHistory")]
    cpu_history: Vec<f32>,
}

#[tokio::main]
async fn main() {
    let app = Router::new()
        .route("/ws/stats", get(ws_handler));

    let addr = SocketAddr::from(([127, 0, 0, 1], 8000));
    println!("Listening on ws://{}", addr);

    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

async fn ws_handler(ws: WebSocketUpgrade) -> impl IntoResponse {
    ws.on_upgrade(handle_socket)
}

async fn handle_socket(mut socket: WebSocket) {
    let mut sys = sysinfo::System::new_all();
    sys.refresh_all();
    
    let mut networks = sysinfo::Networks::new_with_refreshed_list();
    sleep(Duration::from_millis(500)).await;

    let mut cpu_history: Vec<f32> = Vec::new();

    loop {
        sys.refresh_all();
        networks.refresh_list();
        
        let cpu_usage = sys.global_cpu_info().cpu_usage();
        let cpu_model = sys.global_cpu_info().brand().to_string();
        let cpu_cores = sys.cpus().len();

        cpu_history.push(cpu_usage);
        if cpu_history.len() > 10 {
            cpu_history.remove(0);
        }

        let mem_total = sys.total_memory();
        let mem_used = sys.used_memory();
        let mem_usage = if mem_total > 0 {
            (mem_used as f32 / mem_total as f32) * 100.0
        } else {
            0.0
        };

        // disk usage
        let mut disk_total = 0;
        let mut disk_used = 0;
        let disks = sysinfo::Disks::new_with_refreshed_list();
        for disk in &disks {
            let total = disk.total_space();
            let available = disk.available_space();
            disk_total += total;
            disk_used += total.saturating_sub(available);
        }
        let disk_usage = if disk_total > 0 {
            (disk_used as f32 / disk_total as f32) * 100.0
        } else {
            0.0
        };

        // network usage
        let mut rx = 0;
        let mut tx = 0;
        for (_, data) in &networks {
            rx += data.received();
            tx += data.transmitted();
        }

        let os = sysinfo::System::long_os_version().unwrap_or_else(|| "Unknown".to_string());
        let hostname = sysinfo::System::host_name().unwrap_or_else(|| "localhost".to_string());
        let uptime_secs = sysinfo::System::uptime();
        let processes = sys.processes().len();
        
        // Windows fallback for load average using current CPU usage
        let load_str = format!("{:.1}%", cpu_usage);

        let stats = SystemStats {
            hostname,
            uptime: format_uptime(uptime_secs),
            uptime_seconds: uptime_secs,
            cpu: CpuStats {
                model: cpu_model,
                cores: cpu_cores,
                usage_percent: cpu_usage,
            },
            memory: MemoryStats {
                total: format_bytes(mem_total),
                used: format_bytes(mem_used),
                usage_percent: mem_usage,
            },
            disk: DiskStats {
                total: format_bytes(disk_total),
                used: format_bytes(disk_used),
                usage_percent: disk_usage,
            },
            network: NetworkStats {
                rx: format_bytes(rx),
                tx: format_bytes(tx),
            },
            os,
            processes,
            load_average: load_str,
            status: "online".to_string(),
            last_updated: chrono_now(),
            cpu_history: cpu_history.clone(),
        };

        let msg = match serde_json::to_string(&stats) {
            Ok(json) => json,
            Err(_) => break,
        };

        if socket.send(Message::Text(msg.into())).await.is_err() {
            break;
        }

        sleep(Duration::from_secs(2)).await;
    }
}

fn chrono_now() -> String {
    "2026-05-04T12:00:00.000Z".to_string()
}

fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;
    if bytes >= GB {
        format!("{:.1} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.1} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.1} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} B", bytes)
    }
}

fn format_uptime(secs: u64) -> String {
    let d = secs / 86400;
    let h = (secs % 86400) / 3600;
    let m = (secs % 3600) / 60;
    let s = secs % 60;
    if d > 0 {
        format!("{}d {}h {}m", d, h, m)
    } else if h > 0 {
        format!("{}h {}m {}s", h, m, s)
    } else if m > 0 {
        format!("{}m {}s", m, s)
    } else {
        format!("{}s", s)
    }
}
