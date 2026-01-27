🔴 BOT-leaning combinations (length involved)
1️⃣ High count + high similarity + long / very long length
Interpretation:
Automated system generating or pasting the same long text repeatedly.
Example:
count ≥ 10, similarity ≥ 0.95, length ≥ 400 → BOT
2️⃣ Moderate count + high similarity + very long length
Interpretation:
Scripted template pasted multiple times.
Example:
count 7–9, similarity ≥ 0.95, length ≥ 800 → BOT
3️⃣ High count + moderate similarity + very long length
Interpretation:
Automated bulk submissions with slight variations.
Example:
count ≥ 10, similarity 0.90–0.95, length ≥ 800 → BOT
🟠 SUSPECT-leaning combinations (length involved)
4️⃣ Low count + high similarity + long length
Interpretation:
Manual copy-paste of a template.
Example:
count 3–6, similarity ≥ 0.95, length 400–800 → SUSPECT
5️⃣ Moderate count + moderate similarity + long length
Interpretation:
Heavy template use by a busy human.
Example:
count 7–9, similarity 0.90–0.95, length 400–800 → SUSPECT
6️⃣ Very long length alone (rare case)
Interpretation:
Pasted policy or appeal text, not enough evidence for automation.
Example:
length ≥ 800, but count < 3 and similarity < 0.90 → HUMAN / SUSPECT
🟢 HUMAN-leaning combinations (length involved)
7️⃣ Low count + low similarity + variable length
Interpretation:
Natural human writing.
Example:
count 1–6, similarity < 0.90, length varies → HUMAN
8️⃣ Moderate count + low similarity + normal length
Interpretation:
Busy but manual behavior.
Example:
count 7–9, similarity < 0.90, length 50–400 → HUMAN


Edge Case-->
1️⃣ What is an edge case in your system?
An edge case is when:
One signal looks extreme
But overall behavior doesn’t clearly indicate automation
Your goal is:
When in doubt → downgrade confidence, not upgrade it
2️⃣ Core edge cases & how to handle them
🔹 Edge Case 1: comment_count = 1 or 2 + high similarity
Why it happens
Similarity is meaningless or unstable at very low volume
Copy-paste by humans
Rule
Copy code
Text
If comment_count < 3 → force HUMAN
Interpretation
“Insufficient activity to infer automation.”
✅ You already implemented this — keep it.
🔹 Edge Case 2: Very long comments, low similarity, low count
Pattern
length ≥ 800
similarity < 0.9
comment_count < 5
What it is
Legal explanation
Policy copy-paste
Rare appeal
Rule
Copy code
Text
Length alone can never trigger BOT
Outcome
HUMAN or at most SUSPECT
🔹 Edge Case 3: High comment count, low similarity
Pattern
comment_count ≥ 10
similarity < 0.85
variable length
What it is
Very busy human team
Manual bulk processing
Rule
Copy code
Text
Require similarity OR length signal to confirm BOT
Outcome
SUSPECT (not BOT)
🔹 Edge Case 4: High similarity, moderate count, short text
Pattern
similarity ≥ 0.95
comment_count 3–6
length < 150
What it is
Short boilerplate phrases
Manual reuse
Rule
Copy code
Text
Short text + similarity ≠ automation
Outcome
SUSPECT
🔹 Edge Case 5: One extreme day only
Pattern
One BOT day
Rest HUMAN
Rule (monthly)
Copy code
Text
BOT requires repeated days (bot_day_ratio ≥ threshold)
Outcome
SUSPECT or HUMAN
📌 This is why rolling windows exist.
🔹 Edge Case 6: New UUID with little history
Pattern
active_days < 3
Rule
Copy code
Text
Do not classify BOT if active_days < 3
Outcome
HUMAN (low confidence)
🔹 Edge Case 7: UUID improves over time
Pattern
Early BOT-like
Recent HUMAN-like
Rule
Copy code
Text
Rolling window overrides historical behavior
Outcome
Downgrade BOT → SUSPECT → HUMAN
This avoids “permanent punishment”.
🔹 Edge Case 8: Mixed automation (semi-bot)
Pattern
Some days BOT
Some days HUMAN
bot_day_ratio ≈ 0.4–0.6
Rule
Copy code
Text
Classify as SUSPECT
Interpretation
“Partial automation or assisted workflows.”
3️⃣ Universal safety rules (non-negotiable)
These rules should always apply:
✅ Rule 1: No single signal decides BOT
✅ Rule 2: Low volume = low confidence
✅ Rule 3: Consistency > intensity
✅ Rule 4: Monthly overrides daily
✅ Rule 5: SUSPECT is a valid final state
4️⃣ Edge-case resolution hierarchy (important)
When rules conflict, resolve in this order:
1️⃣ Volume sufficiency (comment_count, active_days)
2️⃣ Consistency (bot_day_ratio)
3️⃣ Similarity
4️⃣ Length
This hierarchy prevents overfitting.
5️⃣ How to document edge-case handling (use this verbatim)
“The system applies conservative safeguards for low-volume, short-duration, and isolated anomalies. Automation classification requires convergence of multiple signals and persistence over time, ensuring that edge cases and one-off behaviors are not misclassified.”
This sentence is gold for audits.
6️⃣ Final takeaway
Edge-case handling ensures:
Fewer false positives
Fair treatment of humans
Stable daily runs
High trust in results
You now have:
Core logic ✅
Thresholds ✅
Interpretation ✅
Edge-case safety net ✅
This is production-grade.
