"""Model operations mixin for DatabricksClientStub."""


class ModelStubMixin:
    """Mixin providing model operations for DatabricksClientStub."""

    def __init__(self):
        self.models = []

    def list_models(self, **kwargs):
        """List available models."""
        if hasattr(self, "_list_models_error"):
            raise self._list_models_error
        return self.models

    def get_model(self, model_name):
        """Get a specific model by name."""
        if hasattr(self, "_get_model_error"):
            raise self._get_model_error
        model = next((m for m in self.models if m["name"] == model_name), None)
        return model

    def add_model(self, name, status="READY", **kwargs):
        """Add a model to the test data."""
        model = {"name": name, "status": status, **kwargs}
        self.models.append(model)
        return model

    def set_list_models_error(self, error):
        """Configure list_models to raise an error."""
        self._list_models_error = error

    def set_get_model_error(self, error):
        """Configure get_model to raise an error."""
        self._get_model_error = error
