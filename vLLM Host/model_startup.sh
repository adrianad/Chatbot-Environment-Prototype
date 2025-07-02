nohup vllm serve Qwen/Qwen3-Embedding-4B --port 8001 --task embed --download-dir tmp/ --tensor-parallel-size 2 > output/embedding_output.txt  2>&1 &
echo "wait 30 seconds to give embedding model enough time"
sleep 30

nohup vllm serve Qwen/Qwen3-30B-A3B-FP8 --download-dir tmp/ --gpu-memory-utilization 0.9 --enable-reasoning --reasoning-parser deepseek_r1 --enable-auto-tool-choice --tool-call-parser hermes --tensor-parallel-size 2 --rope-scaling '{"rope_type":"yarn","factor":4.0,"original_max_position_embeddings":32768}' --max-model-len 131072 > output/llm_output.txt 2>&1 &

nohup litellm --config litellm_config.yaml > output/litellm_output.txt 2>&1 &
