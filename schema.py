from pydantic import BaseModel


class PremierLeagueModel(BaseModel):
    name: str
    jersey_number: float
    club: str
    position: str
    nationality: str
    age: float
    appearances:int
    wins: int
    losses: int
    goals: int
    goals_per_match: float
    goals_right_foot: int
    goals_left_foot: int



class StockModel(BaseModel):
    ticker: str


