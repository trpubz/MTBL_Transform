import math

import pandas as pd

pd.options.mode.chained_assignment = None  # silences SettingWithCopyWarning


class Appraiser:
    def __init__(self, ruleset: dict, no_managers: int, budget_split: dict, **kwargs):
        self.ruleset = ruleset
        self.no_managers = no_managers
        self.lg_budget = ruleset["DRAFT_BUDGET"] * no_managers
        self.budget_split = budget_split

        self.pos_groups = {}
        for pos, pos_group in kwargs["bats"].items():
            self.pos_groups[pos] = {}
            self.pos_groups[pos]["players"] = pos_group["players"]
            self.pos_groups[pos]["pool_size"] = (
                    no_managers * ruleset["ROSTER_REQS"]["BATTERS"][pos])

        roster_rps, roster_sps = bucket_wildcard_arms(ruleset)
        for pos, pos_group in kwargs["arms"].items():
            self.pos_groups[pos] = {}
            self.pos_groups[pos]["players"] = pos_group["players"]
            self.pos_groups[pos]["pool_size"] = (
                    no_managers * (roster_sps if pos == "SP" else roster_rps))

        self.lg_category_totals = {}
        # TODO: find league total for each stat category -- store in dictionary
        self.calculate_league_category_totals()
        # TODO: find category total for each position group -- store weighted contribution in
        #  dictionary
        # TODO: set cat_shekel value for each category in each position group -- store in dictionary

    def calculate_league_category_totals(self):
        # establish the categories
        for pos_type in self.ruleset["SCORING"]:
            self.lg_category_totals[pos_type] = {}
            for cat in self.ruleset["SCORING"][pos_type]:
                self.lg_category_totals[pos_type][cat] = 0

        for pos, pos_group in self.pos_groups.items():
            pos_type = "PITCHING" if pos in ["SP", "RP"] else "BATTING"
            num_players = pos_group["pool_size"]
            for cat in self.ruleset["SCORING"][pos_type]:
                self.lg_category_totals[pos_type][cat] += pos_group["players"].loc[
                                                            :num_players, cat].sum()

    def add_skekels(self):
        """"
        Assigns a budget to each player based on their z-score contribution to the position group
        stat category relative to the league z-score total for that category.
        Let's use dingers as our example.  The total amount of HRs in the league needs to be
        calculated; and let's say that the league total is 100.  If 1B position group total has
        20 HRs, then the 1B group gets allocated 20% of the budget for that category.  Let's
        assume there are 5 categories, and there is an even budget distribution for the
        categories: you take the hitter budget (say 65% of a team's spend) and divide it by 5 for
        each of the categories and multiply that by the position group's weighted contribution to
        that category -- for HR for 1B:
        260 (budget) * 10 (teams) == 2600.
        2600 * .65 (batter budget) == 1690.
        1690 / 5 (categories split evenly) == 338 (for spend on HR).
        338 * .2 (1B contribution to HR) == 67.6 (for spend on 1B - HR).
        67.6 / sum(z_proj_HR within 1B) == shekel value per z_HR for 1B.
        -------------------
        Let's assume the sum of z_HR for 1B is 10.  Then the shekel value per z_HR for 1B is 6.76.
        Let's say Big Papi has a z_HR of 3. Then Big Papi's shekel value is 20.28 (which still
        needs to be added to the other categories.
        """
        lg_tot_z = 0
        # initial loop sets the league total z
        for pos, pos_group in self.pos_groups.items():
            df_key = list(pos_group.keys())[0]
            pos_pool = pos_group[df_key]
            pos_pool_z = 0
            # calculate total z for whole league
            for i in range(0, pos_group["pool_size"]):
                lg_tot_z += pos_pool.iloc[i].z_total
                pos_pool_z += pos_pool.iloc[i].z_total

            self.pos_groups[pos]["pool_z"] = pos_pool_z

        # second loop sets the percentage of allocated budget for the pos group
        for pos, pos_group in self.pos_groups.items():
            # store a reference to the instance attribute
            pd.options.mode.copy_on_write = False
            all_pos_players = pos_group["players"]
            # create an empty column
            all_pos_players.loc[:, "shekels"] = pd.Series([], dtype="float64")
            # slice access
            pos_pool = all_pos_players[:pos_group["pool_size"]].copy()
            # get budget allocated to group
            # assign shekels weighted for the players' contribution
            budget_alloc_for_pos_group = (pos_group["pool_z"] / lg_tot_z) * self.lg_budget
            self.pos_groups[pos]["budget_alloc"] = budget_alloc_for_pos_group
            pos_pool.loc[:, "shekels"] = (pos_pool["z_total"] / pos_group["pool_z"] *
                                          budget_alloc_for_pos_group).round(1)
            # pos_pool["shekels"] = pos_pool["shekels"].apply(lambda x: round(x))
            # reinsert shekeled players to instance attribute
            all_pos_players.iloc[:pos_group["pool_size"]] = pos_pool

