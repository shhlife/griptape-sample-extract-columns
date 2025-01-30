# Griptape Sample Structure Template

This is an example structure template that can be used to run a Griptape Agent or publish the output from generic Python Code.

## How it works

In `structure.py` you can set the `use_structure` parameter to `True` or `False`.

If `True`, it will:
1. Set up the Cloud Listener to automatically publish Griptape Structure Events
2. Run the Griptape Structure (in this case an Agent)

```python
# Setup the cloud listener before Griptape Structure
setup_cloud_listener()

# Run the Griptape Structure
agent = Agent()
agent.run(prompt)
```

If `False`, it will:

1. Create two text artifacts - one saing "Hello from a Griptape Cloud Structure." and the second just echoing back whatever was submitted as a prompt
2. Creates both Input and Output artifacts.
3. Set up the Cloud Listener
4. Create a `FinishStructureRunEvent`
5. Publish the `FinishStructureRunEvent`

```python
# Run whatever code you want and make sure to save the output(s) as a TextArtifact(s)
output_artifact_msg = TextArtifact("Hello from a Griptape Cloud Structure.")
output_artifact_prompt = TextArtifact(prompt)

# Create Input and Output Artifacts
task_input = TextArtifact(value=None)
task_output = ListArtifact([output_artifact_msg, output_artifact_prompt])
print(task_output)

# Setup the cloud listener after your code, and before
# publishing the FinishStructureRunEvent
setup_cloud_listener()

# Create the FinishStructureRunEvent
done_event = FinishStructureRunEvent(
    output_task_input=task_input, output_task_output=task_output
)

# Publish the FinishStructureRunEvent
EventBus.publish_event(done_event, flush=True)
```

## How to use it

Choose the **Use This Template** button to create a new GitHub repository with this template.

## Requirements

The structure requires two API keys:

* [Open AI Key](https://platform.openai.com/api-keys)
* [Griptape Cloud Key](https://cloud.griptape.ai/configuration/api-keys)

## Configuration

Save the following keys in you `.env` if running locally, or add them to the structure if running in Griptape Cloud.

```.env
OPENAI_API_KEY=<encrypted_value> # Fill in with your own key
GT_CLOUD_API_KEY=<encrypted_value> # Fill in with your own key
```
