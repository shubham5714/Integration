from typing import Annotated
from pydantic import Field
from tracecat_registry import registry, RegistrySecret, secrets


secret_name = RegistrySecret(name="secret_name", keys=["SECRET_NAME"])


@registry.register(
    default_title="Say Hello",
    display_group="Greetings",
    description="This is a function that says hello",
    namespace="integrations.greetings",
)
def say_hello(name: Annotated[str, Field(description="The name to say hello to")]):
    return {"message": f"Hello, {name}!"}


@registry.register(
    default_title="Say Goodbye",
    display_group="Greetings",
    description="This is a function that says goodbye",
    namespace="integrations.greetings",
)
def say_goodbye(name: Annotated[str, Field(description="The name to say goodbye to")]):
    return {"message": f"Goodbye, {name}!"}


@registry.register(
    default_title="Say Goodbye Secretly",
    display_group="Greetings",
    description="This is a function that says goodbye",
    namespace="integrations.greetings",
    secrets=[secret_name],
)
def say_goodbye_secretly(
    name: Annotated[str, Field(description="The name to say goodbye to")],
):
    secret = secrets.get("SECRET_NAME")
    # For demonstration purposes, we're just returning the secret here.
    return {"message": f"Goodbye, {name}! Secret: {secret}"}
