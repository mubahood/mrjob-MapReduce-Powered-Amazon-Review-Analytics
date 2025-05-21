# mrjob-MapReduce-Powered-Amazon-Review-Analytics
mrjob – MapReduce-Powered: Amazon Review Analytics
Overview

This project uses Python’s mrjob library to perform large-scale, distributed analytics on Amazon review data. We focus on counting and summarizing reviews per product (ASIN) across multiple categories to demonstrate the power and scalability of the MapReduce paradigm.

⸻

Data Sources & Selection
	•	Dataset Host: https://amazon-reviews-2023.github.io
	•	Year: 2023 (latest available)
	•	Categories Chosen:
	•	Subscription_Boxes (~16 K reviews)
	•	Home_and_Kitchen (~67 M reviews)
	•	Digital_Music (~130 K reviews)
	•	Video_Games (~4.6 M reviews)
	•	Books (~29.5 M reviews)

We selected a small, medium, and large dataset to:
	1.	Prototype quickly on a tiny set
	2.	Validate on mid-scale sets
	3.	Stress-test on tens of millions of records
