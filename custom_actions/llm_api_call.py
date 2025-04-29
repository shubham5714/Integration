from typing import Annotated
from typing_extensions import Doc
from tracecat_registry import registry, RegistrySecret, secrets

# Define OpenAI API secret
openai_secret = RegistrySecret(
    name="openai",
    keys=["API_KEY"],
)

@registry.register(
    default_title="Call OpenAI API",
    description="Makes a request to OpenAI API",
    display_group="AI",
    namespace="integrations.openai",
    secrets=[openai_secret],
)
def call_openai_api(
    prompt: Annotated[str, Doc("The prompt to send to OpenAI")],
    model: Annotated[str, Doc("The model to use")] = "gpt-3.5-turbo",
):
    from openai import OpenAI

    # Initialize OpenAI client with secret
    client = OpenAI(api_key=secrets.get("API_KEY"))

    # Make chat completion request
    response = client.chat.completions.create(
        model=model,
        messages=[{"role": "user", "content": prompt}]
    )

    return {
        "response": response.choices[0].message.content
    }
