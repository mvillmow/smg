//! Runtime configuration for the mock worker fleet, parsed from CLI flags.

use std::time::Duration;

use crate::{engine::EngineParams, sim::SimParams};

/// Configuration shared by every mocked HTTP and gRPC worker in the process.
#[derive(Debug, Clone)]
pub struct Config {
    /// Bind address for every listener.
    pub host: String,
    /// First HTTP port; `http_count` workers bind `[http_base_port, +count)`.
    pub http_base_port: u16,
    /// Number of HTTP workers to start.
    pub http_count: u16,
    /// First gRPC port; `grpc_count` workers bind `[grpc_base_port, +count)`.
    pub grpc_base_port: u16,
    /// Number of gRPC workers to start.
    pub grpc_count: u16,
    /// Frontend handshake `ipc://` address ZMQ mock engines connect to (they
    /// dial the SMG/test frontend, which binds the sockets).
    pub zmq_handshake: Option<String>,
    /// Number of ZMQ mock EngineCore ranks to start (each a DP rank).
    pub zmq_count: u16,
    /// Engine index of the first ZMQ rank; ranks use `[start, start+count)`.
    pub zmq_start_index: u32,
    /// Model id advertised by every worker (one model, many replicas).
    pub model_id: String,
    /// Tokenizer path advertised by gRPC workers (for gateway autoload).
    pub tokenizer_path: String,
    /// Simulated per-request generation latency (canned mode only).
    pub gen_delay: Duration,
    /// Number of canned output tokens per generation; also the default output
    /// length for realistic mode when a request omits `max_tokens`.
    pub output_tokens: u32,
    /// When true, each worker runs the realistic continuous-batching engine
    /// simulator ([`EngineParams`]); when false, the cheap canned path.
    pub realistic: bool,
    /// When true, each HTTP worker runs the scale-simulation engine
    /// ([`SimParams`]): SGLang-native `/generate`, prefix-cache accounting,
    /// analytic timing. gRPC/ZMQ workers fall back to canned behavior.
    pub sim: bool,
    /// Engine-simulator parameters (only used when `realistic`).
    pub engine: EngineParams,
    /// Scale-simulation parameters (only used when `sim`). The shared
    /// `--prefill-tps/--max-running/--kv-tokens/--block-size` flags write
    /// both engine structs, so each mode keeps its own defaults while an
    /// explicit flag applies to whichever mode runs.
    pub sim_params: SimParams,
}

impl Config {
    /// Parse the configuration from `std::env::args`, falling back to defaults.
    pub fn from_args() -> Result<Self, String> {
        let mut cfg = Self {
            host: "127.0.0.1".to_string(),
            http_base_port: 9000,
            http_count: 0,
            grpc_base_port: 0,
            grpc_count: 0,
            zmq_handshake: None,
            zmq_count: 0,
            zmq_start_index: 0,
            model_id: "mock-model".to_string(),
            tokenizer_path: String::new(),
            gen_delay: Duration::from_millis(0),
            output_tokens: 8,
            realistic: false,
            sim: false,
            engine: EngineParams::default(),
            sim_params: SimParams::default(),
        };

        let mut args = std::env::args().skip(1);
        while let Some(flag) = args.next() {
            match flag.as_str() {
                "--host" => cfg.host = value(&mut args, &flag)?,
                "--http-base-port" => cfg.http_base_port = parse(value(&mut args, &flag)?, &flag)?,
                "--http-count" => cfg.http_count = parse(value(&mut args, &flag)?, &flag)?,
                "--grpc-base-port" => cfg.grpc_base_port = parse(value(&mut args, &flag)?, &flag)?,
                "--grpc-count" => cfg.grpc_count = parse(value(&mut args, &flag)?, &flag)?,
                "--zmq-handshake" => cfg.zmq_handshake = Some(value(&mut args, &flag)?),
                "--zmq-count" => cfg.zmq_count = parse(value(&mut args, &flag)?, &flag)?,
                "--zmq-start-index" => {
                    cfg.zmq_start_index = parse(value(&mut args, &flag)?, &flag)?
                }
                "--model" => cfg.model_id = value(&mut args, &flag)?,
                "--tokenizer" => cfg.tokenizer_path = value(&mut args, &flag)?,
                "--gen-ms" => {
                    cfg.gen_delay = Duration::from_millis(parse(value(&mut args, &flag)?, &flag)?);
                }
                "--output-tokens" => cfg.output_tokens = parse(value(&mut args, &flag)?, &flag)?,
                "--engine" => match value(&mut args, &flag)?.as_str() {
                    "realistic" => (cfg.realistic, cfg.sim) = (true, false),
                    "sim" => (cfg.realistic, cfg.sim) = (false, true),
                    "canned" => (cfg.realistic, cfg.sim) = (false, false),
                    other => {
                        return Err(format!(
                            "--engine must be canned|realistic|sim, got {other}"
                        ))
                    }
                },
                "--sim-itl-ms" => {
                    cfg.sim_params.itl_ms = parse(value(&mut args, &flag)?, &flag)?;
                }
                "--sim-ttft-base-ms" => {
                    cfg.sim_params.ttft_base_ms = parse(value(&mut args, &flag)?, &flag)?;
                }
                "--image-placeholder-id" => {
                    cfg.sim_params.image_placeholder_id = parse(value(&mut args, &flag)?, &flag)?;
                }
                "--image-tokens-per-image" => {
                    cfg.sim_params.image_tokens_per_image = parse(value(&mut args, &flag)?, &flag)?;
                }
                "--image-bytes-per-token" => {
                    cfg.sim_params.image_bytes_per_token = parse(value(&mut args, &flag)?, &flag)?;
                }
                "--prefill-tps" => {
                    let tps: f64 = parse(value(&mut args, &flag)?, &flag)?;
                    cfg.engine.prefill_tps = tps;
                    cfg.sim_params.prefill_tps = tps;
                }
                "--decode-base-ms" => {
                    cfg.engine.decode_base_ms = parse(value(&mut args, &flag)?, &flag)?;
                }
                "--decode-per-req-ms" => {
                    cfg.engine.decode_per_req_ms = parse(value(&mut args, &flag)?, &flag)?;
                }
                "--prefill-chunk" => {
                    cfg.engine.prefill_chunk_tokens = parse(value(&mut args, &flag)?, &flag)?;
                }
                "--max-running" => {
                    let n: usize = parse(value(&mut args, &flag)?, &flag)?;
                    cfg.engine.max_running = n;
                    cfg.sim_params.max_running = n;
                }
                "--kv-tokens" => {
                    let n: u64 = parse(value(&mut args, &flag)?, &flag)?;
                    cfg.engine.kv_capacity_tokens = n;
                    cfg.sim_params.kv_capacity_tokens = n;
                }
                "--block-size" => {
                    let n: u32 = parse(value(&mut args, &flag)?, &flag)?;
                    cfg.engine.block_size = n;
                    cfg.sim_params.block_size = n as usize;
                }
                "--prefix-cache" => {
                    cfg.engine.prefix_cache = parse(value(&mut args, &flag)?, &flag)?
                }
                "-h" | "--help" => return Err(usage()),
                other => return Err(format!("unknown flag: {other}\n\n{}", usage())),
            }
        }

        if cfg.tokenizer_path.is_empty() {
            cfg.tokenizer_path = cfg.model_id.clone();
        }
        // `--output-tokens` doubles as the realistic engine's default output
        // length when a request omits `max_tokens`.
        cfg.engine.max_new_default = cfg.output_tokens;
        if cfg.http_count == 0 && cfg.grpc_count == 0 && cfg.zmq_count == 0 {
            return Err(format!(
                "nothing to do: pass --http-count, --grpc-count, and/or --zmq-count\n\n{}",
                usage()
            ));
        }
        if cfg.grpc_count > 0 && cfg.grpc_base_port == 0 {
            return Err("--grpc-base-port is required when --grpc-count > 0".to_string());
        }
        if cfg.zmq_count > 0 && cfg.zmq_handshake.is_none() {
            return Err("--zmq-handshake <ipc://…> is required when --zmq-count > 0".to_string());
        }
        Ok(cfg)
    }
}

fn value(args: &mut impl Iterator<Item = String>, flag: &str) -> Result<String, String> {
    args.next()
        .ok_or_else(|| format!("missing value for {flag}"))
}

fn parse<T: std::str::FromStr>(raw: String, flag: &str) -> Result<T, String> {
    raw.parse()
        .map_err(|_| format!("invalid value for {flag}: {raw}"))
}

fn usage() -> String {
    "mock-worker — multi-port mock HTTP/gRPC inference workers for SMG scale testing\n\n\
     Flags:\n\
       --host <addr>            bind address (default 127.0.0.1)\n\
       --http-base-port <port>  first HTTP port (default 9000)\n\
       --http-count <n>         number of HTTP workers (default 0)\n\
       --grpc-base-port <port>  first gRPC port (required if --grpc-count > 0)\n\
       --grpc-count <n>         number of gRPC workers (default 0)\n\
       --zmq-handshake <addr>   frontend ipc:// handshake addr (required if --zmq-count > 0)\n\
       --zmq-count <n>          number of ZMQ mock EngineCore ranks (default 0)\n\
       --zmq-start-index <n>    engine index of the first ZMQ rank (default 0)\n\
       --model <id>             advertised model id (default mock-model)\n\
       --tokenizer <path>       tokenizer path for gRPC autoload (default = model)\n\
       --gen-ms <ms>            canned per-request latency (default 0)\n\
       --output-tokens <n>      output tokens per request when unspecified (default 8)\n\
     \n\
     Realistic engine simulator (continuous batching; opt-in):\n\
       --engine <canned|realistic|sim>  engine mode (default canned)\n\
       --prefill-tps <f>        prefill throughput, tokens/sec (default 8000)\n\
       --decode-base-ms <f>     fixed decode-step latency, ms (default 6.0)\n\
       --decode-per-req-ms <f>  added decode-step latency per running req (default 0.35)\n\
       --prefill-chunk <n>      max prompt tokens prefilled per step (default 2048)\n\
       --max-running <n>        max concurrent running requests (default 256)\n\
       --kv-tokens <n>          KV cache capacity in tokens (default 524288)\n\
       --block-size <n>         cache block/page size in tokens (default 16)\n\
       --prefix-cache <bool>    enable prefix caching + KV events (default true)\n\
     \n\
     Scale-simulation engine (SGLang-native /generate; HTTP only; opt-in):\n\
       --sim-itl-ms <f>              per-output-token latency, ms (default 43)\n\
       --sim-ttft-base-ms <f>        fixed pre-first-token overhead, ms (default 30)\n\
       --image-placeholder-id <id>   image placeholder token id (default 151655)\n\
       --image-tokens-per-image <n>  appended tokens per extra image; 0 = by size (default 0)\n\
       --image-bytes-per-token <n>   payload bytes per derived image token (default 2800)\n\
       (shares --prefill-tps --max-running --kv-tokens --block-size; sim\n\
        defaults: kv 1200000 tokens, block 128)"
        .to_string()
}
