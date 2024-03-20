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

        wild_card_pitchers = ruleset["ROSTER_REQS"]["PITCHERS"]["P"]
        roster_sps = ruleset["ROSTER_REQS"]["PITCHERS"]["SP"]
        wild_card_sps = math.ceil(wild_card_pitchers / 2)  # split the wildcard between SPs and RPs
        roster_sps += wild_card_sps
        wild_card_pitchers -= wild_card_sps
        roster_rps = ruleset["ROSTER_REQS"]["PITCHERS"]["RP"] + wild_card_pitchers
        for pos, pos_group in kwargs["arms"].items():
            self.pos_groups[pos] = {}
            self.pos_groups[pos]["players"] = pos_group["players"]
            self.pos_groups[pos]["pool_size"] = (
                    no_managers * (roster_sps if pos == "SP" else roster_rps))

    def add_skekels(self):
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



