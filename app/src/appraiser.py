import pandas as pd

from app.src.transformer import bucket_wildcard_arms

pd.options.mode.chained_assignment = None  # silences SettingWithCopyWarning


class Appraiser:
    def __init__(self, ruleset: dict, no_managers: int, budget_split: dict, **kwargs):
        self.ruleset = ruleset
        self.no_managers = no_managers
        self.lg_budget = ruleset["DRAFT_BUDGET"] * no_managers

        if budget_is_valid(budget_split):
            self.budget_split = budget_split
        else:
            raise ValueError("Budget split is invalid.")

        self.pos_groups = {}
        for pos, pos_group in kwargs["bats"].items():
            self.pos_groups[pos] = {}
            self.pos_groups[pos]["players"] = pos_group["players"]
            self.pos_groups[pos]["pool_size"] = (
                    no_managers * ruleset["ROSTER_REQS"]["BATTERS"][pos])

        roster_sps, roster_rps = bucket_wildcard_arms(ruleset)
        for pos, pos_group in kwargs["arms"].items():
            self.pos_groups[pos] = {}
            self.pos_groups[pos]["players"] = pos_group["players"]
            self.pos_groups[pos]["pool_size"] = (
                    no_managers * (roster_sps if pos == "SP" else roster_rps))

        self.lg_category_totals = {}
        # TODO: set cat_shekel value for each category in each position group -- store in dictionary
        # TODO: figure out how to properly value ERA and WHIP for pitchers since a lower
        #  percentage of the category is better

    def calculate_league_batting_category_totals(self):
        """
        PITCHING pos_groups are top level in the dict, so there is no "TOTALS" key for them.
        :return: None
        """
        # instantiate the categories dict
        self.lg_category_totals["BATTING"] = {"TOTALS": {}}  # BATTING
        for cat in self.ruleset["SCORING"]["BATTING"]:
            self.lg_category_totals["BATTING"]["TOTALS"][cat] = 0
        # get league and pos_group totals
        for pos, pos_group in self.pos_groups.items():
            if pos not in ["SP", "RP"]:
                num_players = pos_group["pool_size"]
                self.lg_category_totals["BATTING"][pos] = {}
                for cat in self.ruleset["SCORING"]["BATTING"]:
                    proj_cat = f"proj_{cat}"
                    # #loc slicing is inclusive, need to subtract 1
                    pos_group_cat_tot = pos_group["players"].loc[:num_players - 1, proj_cat].sum()
                    self.lg_category_totals["BATTING"]["TOTALS"][cat] += pos_group_cat_tot
                    self.lg_category_totals["BATTING"][pos][cat] = pos_group_cat_tot

    def calculate_pitching_category_weights_shekels(self):
        """
        Pitchers don't have positional weights since sps and rps are shredded separately; only
        calculating category weights and shekels. the lg_category_totals doesn't yet have a key
        for pitchers
        :return: None
        """
        self.lg_category_totals["SP"] = {}
        self.lg_category_totals["RP"] = {}
        for pos in ["SP", "RP"]:
            budget_group = "sps" if pos == "SP" else "rps"
            num_players = self.pos_groups[pos]["pool_size"]
            for cat in self.ruleset["SCORING"]["PITCHING"]:
                proj_cat = f"proj_{cat}"
                if proj_cat in self.pos_groups[pos]["players"].columns:
                    # create dynamic key
                    cat_shekel_per_z = f"{cat}_shekel_per_z"
                    total_cat_z = self.pos_groups[pos]["players"].loc[
                                  :num_players - 1, f"z_{proj_cat}"].sum()
                    self.lg_category_totals[pos][cat_shekel_per_z] = (
                            (self.lg_budget * self.budget_split[budget_group]["ovr"] *
                             self.budget_split[budget_group]["cats"][cat]) /
                            total_cat_z)

    def calculate_batting_category_weights_shekels(self):
        """
        After league totals have been calculated, this method will calculate the weighted
        contribution of each position group to each category.
        :return: None
        """
        for pos, pos_group in self.pos_groups.items():
            if pos not in ["SP", "RP"]:
                for cat in self.ruleset["SCORING"]["BATTING"]:
                    if f"proj_{cat}" not in pos_group["players"].columns:
                        continue
                    # create dynamic keys
                    w_cat = f"w_{cat}"
                    z_proj_cat = f"z_proj_{cat}"  # found in players dataframe
                    self.lg_category_totals["BATTING"][pos][w_cat] = (
                            self.lg_category_totals["BATTING"][pos][cat] /
                            self.lg_category_totals["BATTING"]["TOTALS"][cat])
                    # with the weights set, we can now calculate the shekel value per z for each pos
                    # group by taking the weighted contribution of the pos group to the category,
                    # multiplying it by the budget allocated to the category, and dividing by the
                    # sum of the z values for that category within the pos group.

                    # create dynamic key
                    cat_shekel_per_z = f"{cat}_shekel_per_z"
                    pos_w_cat = self.lg_category_totals["BATTING"][pos][w_cat]
                    cat_budget = (self.lg_budget *
                                  self.budget_split["bats"]["ovr"] *
                                  self.budget_split["bats"]["cats"][cat])
                    num_players = pos_group["pool_size"]

                    self.lg_category_totals["BATTING"][pos][cat_shekel_per_z] = (
                            (cat_budget * pos_w_cat) / (
                                pos_group["players"].loc[:num_players - 1, z_proj_cat].sum())
                    )

    def add_skekels(self):
        """
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
        needs to be added to the other categories.)
        """
        for pos, pos_group in self.pos_groups.items():
            pos_type = "PITCHING" if pos in ["SP", "RP"] else "BATTING"
            for cat in self.ruleset["SCORING"][pos_type]:
                if f"proj_{cat}" not in pos_group["players"].columns:
                    continue
                # create dynamic key
                z_proj_cat = f"z_proj_{cat}"
                cat_shekel_per_z_key = f"{cat}_shekel_per_z"
                # there is a "BATTING" key at a higher level only for pos players
                cat_shekel_per_z_value = self.lg_category_totals["BATTING"][pos][
                    cat_shekel_per_z_key] if pos_type == "BATTING" \
                    else self.lg_category_totals[pos][cat_shekel_per_z_key]

                pos_group["players"][f"{cat}_shekels"] = (
                        pos_group["players"][z_proj_cat] *
                        cat_shekel_per_z_value)

            pos_group["players"]["shekels"] = pos_group["players"].filter(
                like="_shekels").sum(axis=1)


def budget_is_valid(budget_split: dict) -> bool:
    """
    Checks if the budget split is valid.
    :param budget_split: dict
    :return: bool
    """
    pos_group_budget = 0
    for group, breakdown in budget_split.items():
        pos_group_budget += breakdown["ovr"]
        pos_cat_budget = 0
        for cat, weight in breakdown["cats"].items():
            pos_cat_budget += weight

        if pos_cat_budget != 1:
            return False

    if pos_group_budget != 1:
        return False
    else:
        return True
