"""Create the required Kafka topics if they do not exist."""

from streaming_platform.config import get_settings
from streaming_platform.kafka.admin import ensure_topics


def main() -> None:
    """Bootstrap topics and report whether any were created."""
    created = ensure_topics(get_settings())
    if created:
        print("Created topics: " + ", ".join(created))
    else:
        print("All required topics already exist")


if __name__ == "__main__":
    main()
