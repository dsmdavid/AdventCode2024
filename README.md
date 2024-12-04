# AdventCode2024

![](https://img.shields.io/badge/day%20📅-4-blue)
![](https://img.shields.io/badge/stars%20⭐-8-yellow)
![](https://img.shields.io/badge/days%20completed-4-red)

AoC 2024

---
Here be dragons
---

sql / dbt (duckdb) and maybe python solutions to AoC2024


---
Steps to setup:

- gh actions require secrets (to update stars/check progress, download input)
    - AOC_EMAIL
    - AOC_SESSION
    - AOC_USER_AGENT
    - AOC_USER_ID
 
- to set up locally
`uv venv --python 3.12`
`uv pip install dbt-core dbt-duckdb sqlfluff sqlfluff-templater-dbt black`
(or install from requirements file)
