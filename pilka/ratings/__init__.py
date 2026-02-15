"""

    pilka.ratings
    ~~~~~~~~~~~~~
    Scrape Ekstraklasa players' ratings data.

    @author: z33k

"""
from dataclasses import dataclass
import datetime


@dataclass(frozen=True)
class Country:
    alpha2: str
    alpha3: str
    name: str
    slug: str | None = None


# TODO: decide how much data should be saved
@dataclass(frozen=True)
class Player:
    name: str
    country: Country
    position: str
    jersey_number: int
    height: int
    date_of_birth: datetime.date
    rating: float | None = None
    minutes_played: int | None = None
    goals: int | None = None
    assists: int | None = None
    fouls: int | None = None

    @property
    def is_rated(self) -> bool:
        return self.rating is not None

    @property
    def is_goal_scorer(self) -> bool:
        return self.goals is not None


@dataclass(frozen=True)
class Team:
    name: str
    starters: tuple[Player, ...]
    substitutions: tuple[Player, ...]
    manager: str | None = None

    @property
    def players(self) -> list[Player]:
        return [*self.starters, *self.substitutions]

    @property
    def rating(self) -> float:
        """Return rating for the team expressed as the average rating of its players weighted by
        the minutes they played.
        """
        players = [p for p in self.players if p.is_rated]
        weighted_sum = sum(p.rating * p.minutes_played for p in players)
        total_weight = sum(p.minutes_played for p in players)
        return weighted_sum / total_weight

    @property
    def goals_scored(self) -> int:
        return sum(p.goals for p in self.players if p.is_goal_scorer)


@dataclass(frozen=True)
class Match:
    home_team: Team
    away_team: Team
    stadium: str | None = None
    attendance: int | None = None
    date: datetime.date | None = None

    @property
    def score(self) -> str:
        return f"{self.home_team.goals_scored} : {self.away_team.goals_scored}"

