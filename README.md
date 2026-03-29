**Configuration:**
- **Publications**: 200,000
- **Subscriptions**: 200,000
- **Field Weights**
  - **company**: 0.9
  - **value**: 0.6
  - **drop**: 0.3
  - **variation**: 0.5
  - **date**: 0.2
- **Equality Weights**
  - **company**: 0.7

**Setup:**

	Intel(R) Core(TM) i7-8750H CPU @ 2.20GHz 

	Base speed:	2.20 GHz
	Sockets:	1
	Cores:	6
	Logical processors:	12

**Results**:

- 1 thread
  - Publication Time (s): 0.4096
  - Subscription Time (s): 0.6660
  - Total Time (s): 1.0756

- 4 threads
  - Publication Time (s): 0.3557
  - Subscription Time (s): 0.5043
  - Total Time (s): 0.8600

--------------------------------
**Configuration:**
- **Publications**: 10,000,000
- **Subscriptions**: 10,000,000
- **Field Weights**
  - **company**: 0.9
  - **value**: 0.6
  - **drop**: 0.3
  - **variation**: 0.5
  - **date**: 0.2
- **Equality Weights**
  - **company**: 0.7


**Setup**:

	Apple M4 Pro
	Cores: 14 (8 performance - 4.51 GHz + 6 efficiency - 2.59 GHz)

**Results**:

- 1 thread
  - Publication Generation (s): 1.0556
  - Publication Total (s): 1.9412
  - Subscription Generation (s): 3.6419
  - Subscription Total (s): 5.9787
  - Total Time (s): 7.9199

- 4 threads
  - Publication Generation (s): 1.1718
  - Publication Total (s): 2.2800
  - Subscription Generation (s): 2.6024
  - Subscription Total (s): 4.9609
  - Total Time (s): 7.2409
