# Explain Code

Explains code with visual diagrams and analogies. Use when explaining how code works, teaching about a codebase, or when the user asks "how does this work?"

When explaining code, always include:

## 1) Analogy first

Compare the code to something from everyday life. Make it concrete and relatable before touching implementation details. For complex concepts, use multiple analogies from different angles.

## 2) Diagram

Draw an ASCII diagram showing the flow, structure, or relationships — whichever is most illuminating for the concept at hand.

Examples of what to draw depending on context:
- **Data flow**: how a request/IO moves through layers
- **State machine**: states and transitions (e.g., IDLE → SYNCING → ACTIVE)
- **Object relationships**: ownership, composition, dependencies
- **Sequence**: call order between components over time
- **Memory layout**: struct fields, buffer organization

## 3) Step-by-step walkthrough

Walk through the code explaining what happens at each stage. Reference actual file paths and line numbers (e.g., `src/raid/raid1.cpp:42`) so the user can follow along. Stay grounded in the real code — don't invent behavior.

For this codebase specifically, note relevant context where applicable:
- Whether this is public API (`include/ublkpp/`) or internal (`src/`)
- Which RAID type is involved (RAID0/1/10) and what that means for the logic
- Whether coroutines / `co_await` are in play and what suspension means here
- How `std::expected<T, std::error_condition>` / `io_result` is threaded through

## 4) The gotcha

Call out one common mistake or misconception about this code. What trips people up? What looks obvious but isn't? What assumption does the code rely on that a reader might miss?

## Tone

Keep explanations conversational. Avoid dense walls of text — prefer short paragraphs, bullet points, and the diagram doing heavy lifting. If the concept is genuinely complex, layer the explanation: simple analogy first, then mechanics, then edge cases.
