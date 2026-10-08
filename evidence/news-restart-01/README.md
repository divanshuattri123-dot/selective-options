# Offline production RSS restart and failure recovery

The production parser/collector replays the original saved public Fed entity into actual DuckDB storage. All 16 native records and 15 first headline receipts remain unchanged. Later repeated poll, transport failure and recovery use three separately labelled synthetic health records and simulated clocks, documented in report.json. No new HTTP requests occurred.

Three closed store/sealed archive checks, 32 original event PIT boundaries, 13 canonical quality scenarios and exact pending/local-ack outbox persistence passed. Failure remains blocking after restart; recovery becomes eligible only at its real fixture health boundary. This is software/replay evidence, not a real public outage or continuous-feed run, and authorizes no trading.

The saved original source files are immutable. Re-run python -m scripts.verify_news_restart with a NEW output directory. Parent independently checked native subsets, synthetic identities, all archives and all quality scenarios; see ../restart-parent-validation-0338.json. The copied parent verifier snapshot writes its fixed report with exclusive creation and is preserved as evidence, not a command to overwrite an existing proof.
