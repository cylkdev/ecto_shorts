---
trigger: always_on
---

Rule: Use FactoryEx for Test Data

When inserting records in tests, build data through FactoryEx and not raw Repo calls, to keep fixtures consistent and maintainable.

documentation for factory_ex can be found here:

https://hexdocs.pm/factory_ex/FactoryEx.html