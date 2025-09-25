# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Core Principles

This document defines the design principles and patterns to follow

## 1. Single Responsibility Principle (SRP)

### Rule: One Class, One Purpose
Each class should have exactly one reason to change. Separate concerns into distinct classes.

### ✅ DO:
```python
# Each class has a single, well-defined responsibility
class DLModel:
    def train(self, features):
        # Training logic only
        pass

class Preprocessor:
    def preprocess(self, features):
        # Preprocessing logic only
        pass

class DLEvaluator:
    def evaluate(self, model):
        # Evaluation logic only
        pass
```

### ❌ DON'T:
```python
# Avoid god classes that do everything
class MLPipeline:
    def preprocess(self, data):
        pass
    def train(self, data):
        pass
    def evaluate(self):
        pass
    def save_model(self):
        pass
    def load_model(self):
        pass
```

### Guidelines:
- **Model classes**: Handle only model architecture and forward pass
- **Preprocessor classes**: Handle only data transformation
- **Evaluator classes**: Handle only metrics and evaluation
- **DataLoader classes**: Handle only data loading and batching
- **Trainer classes**: Handle only training loops and optimization

---

## 2. Open/Closed Principle (OCP)

### Rule: Open for Extension, Closed for Modification
Design systems that can be extended with new functionality without modifying existing code.

### ✅ DO:
```python
from abc import ABC, abstractmethod

# Define abstract interface
class Extractor(ABC):
    @abstractmethod
    def extract(self, data):
        pass

# Extend via new classes
class SpectrogramExtractor(Extractor):
    def extract(self, data):
        return spectrogram_logic(data)

class MFCCExtractor(Extractor):
    def extract(self, data):
        return mfcc_logic(data)

# Pipeline works with any extractor
class DLPipeline:
    def __init__(self, extractor: Extractor):
        self.extractor = extractor

    def run(self, data):
        features = self.extractor.extract(data)
        # Process features
```

### ❌ DON'T:
```python
# Avoid switch/if-else chains that need modification
class DLPipeline:
    def extract(self, data, feature_type):
        if feature_type == "spectrogram":
            return extract_spectrogram(data)
        elif feature_type == "mfcc":
            return extract_mfcc(data)
        # Need to modify this method for new feature types
```

### Guidelines:
- Use abstract base classes for extensible components
- Prefer composition over inheritance
- Use dependency injection
- New features should require new classes, not method modifications

---

## 3. Liskov Substitution Principle (LSP)

### Rule: Subtypes Must Be Substitutable
Derived classes must be usable through the base class interface without breaking functionality.

### ✅ DO:
```python
class Extractor(ABC):
    @abstractmethod
    def extract(self, data):
        pass

# All extractors have consistent interface
class MFCCExtractor(Extractor):
    def __init__(self, num_mfccs=13):
        self.num_mfccs = num_mfccs

    def extract(self, data):
        # Configuration via constructor, not method params
        return extract_mfcc(data, self.num_mfccs)
```

### ❌ DON'T:
```python
# Avoid inconsistent method signatures
class MFCCExtractor(Extractor):
    def extract(self, data, num_mfccs):  # Different signature!
        return extract_mfcc(data, num_mfccs)
```

### Guidelines:
- Keep method signatures consistent across implementations
- Move configuration to constructors, not method parameters
- Ensure all implementations fulfill the base class contract
- Don't throw unexpected exceptions in derived classes

---

## 4. Interface Segregation Principle (ISP)

### Rule: Many Specific Interfaces > One General Interface
Clients should not be forced to depend on interfaces they don't use.

### ✅ DO:
```python
# Separate interfaces for different capabilities
class ItemRecommender(ABC):
    @abstractmethod
    def get_closest_items(self, item):
        pass

class PersonalisedRecommender(ABC):
    @abstractmethod
    def get_personalised_recommendations(self, user):
        pass

# Implement only what's needed
class NearestNeighbourRecommender(ItemRecommender):
    def get_closest_items(self, item):
        return find_nearest(item)

# Implement multiple interfaces when needed
class CollaborativeFilteringRecommender(ItemRecommender, PersonalisedRecommender):
    def get_closest_items(self, item):
        return find_similar_items(item)

    def get_personalised_recommendations(self, user):
        return recommend_for_user(user)
```

### ❌ DON'T:
```python
# Avoid fat interfaces
class Recommender(ABC):
    @abstractmethod
    def get_closest_items(self, item):
        pass

    @abstractmethod
    def get_personalised_recommendations(self, user):
        pass

    @abstractmethod
    def explain_recommendation(self, item, user):
        pass

    # Forces all implementations to support all methods
```

### Guidelines:
- Create focused interfaces for specific capabilities
- Use multiple inheritance for classes needing multiple capabilities
- Prefer many small interfaces over few large ones
- Name interfaces by their capability (e.g., Trainable, Evaluable)

---

## 5. Dependency Inversion Principle (DIP)

### Rule: Depend on Abstractions, Not Concretions
High-level modules should not depend on low-level modules. Both should depend on abstractions.

### ✅ DO:
```python
from abc import ABC, abstractmethod

# Define abstraction
class Evaluator(ABC):
    @abstractmethod
    def evaluate(self, model, data):
        pass

# High-level class depends on abstraction
class MLPipeline:
    def __init__(self, evaluator: Evaluator):
        self.evaluator = evaluator

    def run_evaluation(self, model, data):
        return self.evaluator.evaluate(model, data)

# Concrete implementations
class TensorFlowEvaluator(Evaluator):
    def evaluate(self, model, data):
        return tf_evaluate(model, data)

class PyTorchEvaluator(Evaluator):
    def evaluate(self, model, data):
        return torch_evaluate(model, data)
```

### ❌ DON'T:
```python
# Avoid direct dependencies on concrete classes
class MLPipeline:
    def __init__(self):
        self.evaluator = TensorFlowEvaluator()  # Hard dependency!

    def run_evaluation(self, model, data):
        return self.evaluator.evaluate(model, data)
```

### Guidelines:
- Use dependency injection via constructors
- Define interfaces/protocols for all dependencies
- Type hint with abstractions, not concrete classes
- Consider using dependency injection frameworks for complex projects

---

## Additional Design Principles

### 6. DRY (Don't Repeat Yourself)

**Rule**: Every piece of knowledge must have a single, unambiguous, authoritative representation within a system.

### ✅ DO:
```python
# Single source of truth for feature extraction logic
def extract_features(data, feature_type):
    """Centralized feature extraction logic"""
    if feature_type == "mfcc":
        return librosa.feature.mfcc(data)
    elif feature_type == "melspec":
        return librosa.feature.melspectrogram(data)

# Reuse the function everywhere
train_features = extract_features(train_data, config.feature_type)
test_features = extract_features(test_data, config.feature_type)
```

### ❌ DON'T:
```python
# Duplicated logic in multiple places
def train_model(data):
    # Duplicated feature extraction
    features = librosa.feature.mfcc(data, n_mfcc=13, hop_length=512)
    # ... training logic

def evaluate_model(data):
    # Same extraction logic duplicated
    features = librosa.feature.mfcc(data, n_mfcc=13, hop_length=512)
    # ... evaluation logic
```

### Guidelines:
- Extract common logic into functions or methods
- Use configuration objects for shared parameters
- Create utility modules for repeated operations
- **Warning**: Don't DRY too early - wait until you have 3+ repetitions
- Prefer some duplication over wrong abstractions

---

### 7. KISS (Keep It Simple, Stupid)

**Rule**: Most systems work best if they are kept simple rather than made complicated.

### ✅ DO:
```python
# Simple, clear implementation
def calculate_accuracy(predictions, labels):
    correct = (predictions == labels).sum()
    total = len(labels)
    return correct / total

# Straightforward data loading
def load_data(file_path):
    with open(file_path, 'r') as f:
        return json.load(f)
```

### ❌ DON'T:
```python
# Overly complex for no benefit
def calculate_accuracy(predictions, labels):
    return functools.reduce(
        lambda acc, pair: acc + (1 if pair[0] == pair[1] else 0),
        zip(predictions, labels),
        0
    ) / len(labels)

# Over-engineered data loading
class DataLoaderFactory:
    @staticmethod
    def create_loader(loader_type):
        if loader_type == "json":
            return JSONDataLoader()

class JSONDataLoader(AbstractDataLoader):
    def load(self, path, encoding='utf-8', **kwargs):
        return super().load(path, encoding, **kwargs)
```

### Guidelines:
- Start with the simplest solution that works
- Add complexity only when necessary
- Prefer clarity over cleverness
- If you need to comment to explain it, it might be too complex

---

### 8. SoC (Separation of Concerns)

**Rule**: Separate a program into distinct sections, each addressing a separate concern.

### ✅ DO:
```python
# Separate concerns into different modules

# data/loader.py - Data loading concern
class DataLoader:
    def load_batch(self, batch_size):
        pass

# models/architecture.py - Model architecture concern
class ModelArchitecture:
    def forward(self, x):
        pass

# training/optimizer.py - Optimization concern
class Optimizer:
    def step(self, loss):
        pass

# metrics/evaluator.py - Evaluation concern
class Evaluator:
    def calculate_metrics(self, predictions, targets):
        pass
```

### ❌ DON'T:
```python
# Mixed concerns in one class
class MLSystem:
    def load_data(self):
        pass

    def build_model(self):
        pass

    def train(self):
        pass

    def evaluate(self):
        pass

    def visualize(self):
        pass
```

### Guidelines:
- Follow MVC pattern for applications (Model-View-Controller)
- Separate business logic from infrastructure
- Keep data access separate from processing
- Don't split into too many modules - find the right balance

---

## Naming Conventions

### General Rules
- Use meaningful and intention-revealing names
- Prefer long descriptive names over short names with comments
- Be consistent across the codebase
- Don't Use Magic Numbers
- Don't Add Redundant Context


### Functions
- Use Verbs for Function Names
- Use Consistent Words for Same Concepts
- Functions Should Perform Single Tasks
- Keep Arguments to a Minimum
- Avoid Flags in Functions
- Avoid Side Effects

## Comments Guidelines
- Don't Comment Bad Code, Rewrite It
- Readable Code Doesn't Need Comments
- Comments Should Explain "Why", Not "What"
- Use Proper Documentation Strings
- Never Leave Commented-Out Code

## Technology Stack Preferences

Use **Pydantic** over dataclasses for configuration:
Use **Typer** for command-line interfaces:
Use **FastAPI** for web services:

### Why These Choices?

1. **Pydantic**: Provides runtime type validation, JSON schema generation, and better IDE support
2. **Typer**: Modern CLI framework with type hints, automatic help generation, and testing utilities
3. **FastAPI**: Automatic API documentation, async support, built-in validation with Pydantic

---

## Anti-Patterns to Avoid

### Code Structure Anti-Patterns
1. **God Class**: Classes that do too much (violates SRP)
2. **Spaghetti Code**: Code with tangled control flow (violates KISS)
3. **Copy-Paste Programming**: Duplicated code blocks (violates DRY)
4. **Premature Optimization**: Complex code without proven need (violates KISS)
5. **Tight Coupling**: Direct dependencies between modules (violates DIP)

### Naming Anti-Patterns
6. **Magic Numbers**: Hardcoded values in logic
7. **Cryptic Names**: Single letters or meaningless abbreviations
8. **Inconsistent Naming**: Mixed terminology for same concept
9. **Hungarian Notation**: Type prefixes in dynamically typed Python

### Design Anti-Patterns
10. **Leaky Abstractions**: Implementation details in interfaces
11. **Feature Envy**: Methods that use another class's data excessively
12. **Inappropriate Intimacy**: Classes that know too much about each other
13. **Middle Man**: Classes that only delegate to other classes

### ML-Specific Anti-Patterns
14. **Training-Serving Skew**: Different preprocessing in training vs production
15. **Data Leakage**: Using test data information during training
16. **Hardcoded Hyperparameters**: Magic numbers for model configuration
17. **Missing Validation**: No input data validation before model inference

### General Bad Practices
18. **Not Invented Here**: Reimplementing existing solutions
19. **Callback Hell**: Deeply nested callbacks
20. **Global State**: Mutable global variables
21. **Stringly Typed**: Using strings where enums/types are appropriate
22. **Dead Code**: Commented-out or unreachable code

## Commands

### Development Setup
```bash
# Install development dependencies
uv pip install -e ".[dev,cli]"
```

### Testing
```bash
# Run all tests
uv run pytest
```

### Code Quality
```bash
# Auto-fix linting issues
uvx ruff check --fix src/

# Format code
ruff format src/

# Stati type analysis
uvx ruff ty check src/
```
