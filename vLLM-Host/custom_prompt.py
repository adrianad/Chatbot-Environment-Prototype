from litellm.integrations.custom_logger import CustomLogger

class QwenNoThinkLogger(CustomLogger):
    """
    Custom logger to intercept and modify requests for FAST_LLM model
    """
    
    def __init__(self):
        super().__init__()
        # Store which requests need /no_think
        self.fast_llm_requests = set()
    
    def log_pre_api_call(self, model, messages, kwargs):
        # Get the original model name from kwargs if available
        litellm_params = kwargs.get('litellm_params', {})
        
        # Check metadata for model_group
        metadata = litellm_params.get('metadata', {})
        model_group = metadata.get('model_group', '')
        
        # Check if this is a FAST_LLM model
        is_fast_llm = "FAST_LLM" in str(model_group)
        
        if is_fast_llm:
            # Store the call ID for later reference
            call_id = kwargs.get('litellm_call_id', '')
            if call_id:
                self.fast_llm_requests.add(call_id)
            
            # Try to modify the actual prompt that will be sent to vLLM
            # Look for the prompt in additional_args
            additional_args = kwargs.get('additional_args', {})
            if isinstance(additional_args, dict):
                complete_input_dict = additional_args.get('complete_input_dict', {})
                
                # Set a custom prompt if available
                if isinstance(complete_input_dict, dict):
                    # For chat completions, find the user message and prepend /no_think
                    messages = complete_input_dict.get('messages', [])
                    if isinstance(messages, list):
                        for i, message in enumerate(messages):
                            if isinstance(message, dict) and message.get("role") == "user":
                                original_content = message.get('content', '')
                                if not original_content.startswith('/no_think'):
                                    complete_input_dict['messages'][i]["content"] = f"/no_think {original_content}"
                                    break
                    
                    # Also check if there's already a formatted prompt
                    if 'prompt' in complete_input_dict:
                        original_prompt = complete_input_dict['prompt']
                        if '/no_think' not in original_prompt:
                            # Insert /no_think after the user part but before assistant
                            if '<|im_start|>user\n' in original_prompt:
                                modified_prompt = original_prompt.replace(
                                    '<|im_start|>user\n', 
                                    '<|im_start|>user\n/no_think '
                                )
                                complete_input_dict['prompt'] = modified_prompt
            
            # Also modify the main messages parameter
            for i, message in enumerate(messages):
                if message.get("role") == "user":
                    original_content = message.get('content', '')
                    if not original_content.startswith('/no_think'):
                        messages[i]["content"] = f"/no_think {original_content}"
                        break
    
    # Add async versions for proxy compatibility
    async def async_log_pre_api_call(self, model, messages, kwargs):
        # Call the sync version
        self.log_pre_api_call(model, messages, kwargs)
    
    def log_success_event(self, kwargs, response_obj, start_time, end_time):
        call_id = kwargs.get('litellm_call_id', '')
        if call_id in self.fast_llm_requests:
            self.fast_llm_requests.discard(call_id)
    
    async def async_log_success_event(self, kwargs, response_obj, start_time, end_time):
        call_id = kwargs.get('litellm_call_id', '')
        if call_id in self.fast_llm_requests:
            self.fast_llm_requests.discard(call_id)
    
    def log_failure_event(self, kwargs, response_obj, start_time, end_time):
        call_id = kwargs.get('litellm_call_id', '')
        if call_id in self.fast_llm_requests:
            self.fast_llm_requests.discard(call_id)
    
    async def async_log_failure_event(self, kwargs, response_obj, start_time, end_time):
        call_id = kwargs.get('litellm_call_id', '')
        if call_id in self.fast_llm_requests:
            self.fast_llm_requests.discard(call_id)

# Create the instance
logger_callback = QwenNoThinkLogger()
