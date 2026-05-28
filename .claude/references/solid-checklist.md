# SOLID Checklist

## Single Responsibility Principle (SRP)

**Smell prompts:**
- Does this class/module do more than one thing? Can you describe it without using "and"?
- Would changes to two unrelated features both require editing this file?
- Does this class have more than ~200 lines (excluding tests)? What are all the reasons it might change?
- Are there multiple levels of abstraction in one file (e.g., HTTP parsing + business logic + DB access)?

**Refactor heuristic:** Extract cohesive groups of methods into a new class. Pass the extracted class via constructor injection. Ensure the original class delegates rather than duplicates.

---

## Open/Closed Principle (OCP)

**Smell prompts:**
- Are new behaviors added by modifying existing switch/if-else chains rather than adding new types?
- Does adding a new variant require touching the core class, not just adding a subclass/strategy?
- Is there a long switch on a type tag or enum that grows with each feature?

**Refactor heuristic:** Introduce a strategy interface or abstract base. Move each branch into its own implementation. Register implementations via a factory or map rather than inline dispatch.

---

## Liskov Substitution Principle (LSP)

**Smell prompts:**
- Does a subclass override a method to throw `NotImplemented` or do nothing?
- Are there `instanceof` / `dynamic_cast` checks that dispatch on the concrete type?
- Does using a subclass where the base is expected change observable behavior (preconditions, postconditions)?
- Does the subclass narrow an argument type or widen a return type relative to the base?

**Refactor heuristic:** If a subclass can't honor the base contract, the inheritance is wrong. Consider composition over inheritance, or split the base interface.

---

## Interface Segregation Principle (ISP)

**Smell prompts:**
- Do clients implement methods they don't use (stub or `throw NotImplemented`)?
- Is a single interface used across very different contexts, each needing only a subset?
- Does a mock in tests need to stub many unrelated methods just to satisfy the interface?

**Refactor heuristic:** Split the wide interface along client usage boundaries. Each client should depend only on the narrowest interface it needs.

---

## Dependency Inversion Principle (DIP)

**Smell prompts:**
- Does high-level business logic `new` a concrete low-level class directly?
- Are there hard-coded references to concrete implementations (file paths, DB drivers) inside domain logic?
- Is it impossible to unit-test the high-level module without spinning up real infrastructure?

**Refactor heuristic:** Extract an abstract interface for the low-level dependency. Inject the concrete implementation via constructor. The high-level module should own and define the interface, not the low-level module.

---

## General Architecture Smells

- **God object**: One class that knows everything and does everything. Split by responsibility.
- **Feature envy**: A method that uses another class's data more than its own. Move it there.
- **Shotgun surgery**: A single change requires editing many unrelated files. Consolidate the concept.
- **Inappropriate intimacy**: Two classes that reach deeply into each other's internals. Introduce an API boundary.
- **Data clump**: The same group of fields/parameters always appear together. Extract them into a value object.
