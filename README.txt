1. If needed, install and activate the canoe-backend conda environment:
	a. >cd C:/<path to this directory>/
	b. >conda env create
	c. >conda activate canoe-backend

2. Add sqlite to be sorted/validated to input_sqlite.

3. Run validate_sort_period_vintage.py:
	a. >python validate_sort_period_vintage.py

4. Invalidly indexed rows will be printed out by table. Error codes:
	~exs: 0 capacity or missing row in ExistingCapacity
	~eff: does not have a corresponding row in the Efficiency table
	~per: period is not in modelled ('f' tag) time periods
	~vint: vintage is not in time periods
	v>p: vintage is greater than period (tech not built in time for this period)
	v+l<=p: vintage + life <= period (tech would retire before this period)

5. The sorted sqlite db is found in output_sqlite. Error rows are identified but not fixed.