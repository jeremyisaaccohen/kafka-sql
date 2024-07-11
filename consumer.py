import numpy as np
from kafka import KafkaConsumer
from constants import TOPIC_NAME, BOOTSTRAP_SERVERS
import json
import math

consumer = KafkaConsumer(TOPIC_NAME, bootstrap_servers=[BOOTSTRAP_SERVERS], auto_offset_reset='earliest')

### Write to SQL?
import psycopg2


def parse_message(message):
    print("Consumer received", message)
    message = json.loads(message.value.decode())
    data = {k: None if isinstance(v, float) and math.isnan(v) else v for k, v in message.items()}
    keys = tuple(message.keys())
    vals = tuple(data.values())

    # Function to format the values
    def format_value(value):
        if isinstance(value, str):
            if value.endswith('%'):
                return str(float(value.strip('%')) / 100)  # Convert percentage string to float
            return "'{}'".format(value.replace("'",
                                               "''"))  # Enclose string in single quotes and escape single quotes within the string
        elif value is None:
            return 'NULL'  # Convert None to NULL
        return str(value)  # Leave other values as is

    # Format the values for SQL
    return ', '.join([format_value(v) for v in vals])

with psycopg2.connect(
    "dbname='postgres' user='postgres' host='localhost' port='5432'"
) as conn:
    conn.autocommit = True
    # Your database operations
    print("Connected to the database.")

    with conn.cursor() as cursor:
        i = 0
        cursor.execute("""CREATE TABLE IF NOT EXISTS footy (
        name VARCHAR(100),
        jersey_number INTEGER,
        club VARCHAR(100),
        position VARCHAR(50),
        nationality VARCHAR(50),
        age INTEGER,
        appearances INTEGER,
        wins INTEGER,
        losses INTEGER,
        goals INTEGER,
        goals_per_match FLOAT,
        headed_goals INTEGER,
        goals_with_right_foot INTEGER,
        goals_with_left_foot INTEGER,
        penalties_scored INTEGER,
        freekicks_scored INTEGER,
        shots INTEGER,
        shots_on_target INTEGER,
        shooting_accuracy_percent FLOAT,
        hit_woodwork INTEGER,
        big_chances_missed INTEGER,
        clean_sheets INTEGER,
        goals_conceded INTEGER,
        tackles INTEGER,
        tackle_success_percent FLOAT,
        last_man_tackles INTEGER,
        blocked_shots INTEGER,
        interceptions INTEGER,
        clearances INTEGER,
        headed_clearance INTEGER,
        clearances_off_line INTEGER,
        recoveries INTEGER,
        duels_won INTEGER,
        duels_lost INTEGER,
        successful_50_50s INTEGER,
        aerial_battles_won INTEGER,
        aerial_battles_lost INTEGER,
        own_goals INTEGER,
        errors_leading_to_goal INTEGER,
        assists INTEGER,
        passes INTEGER,
        passes_per_match FLOAT,
        big_chances_created INTEGER,
        crosses INTEGER,
        cross_accuracy_percent FLOAT,
        through_balls INTEGER,
        accurate_long_balls INTEGER,
        saves INTEGER,
        penalties_saved INTEGER,
        punches INTEGER,
        high_claims INTEGER,
        catches INTEGER,
        sweeper_clearances INTEGER,
        throw_outs INTEGER,
        goal_kicks INTEGER,
        yellow_cards INTEGER,
        red_cards INTEGER,
        fouls INTEGER,
        offsides INTEGER
    );
    """)

        key_string = "name, jersey_number, club, position, nationality, age, appearances, wins, losses, goals, goals_per_match, headed_goals, goals_with_right_foot, goals_with_left_foot, penalties_scored, freekicks_scored, shots, shots_on_target, shooting_accuracy_percent, hit_woodwork, big_chances_missed, clean_sheets, goals_conceded, tackles, tackle_success_percent, last_man_tackles, blocked_shots, interceptions, clearances, headed_clearance, clearances_off_line, recoveries, duels_won, duels_lost, successful_50_50s, aerial_battles_won, aerial_battles_lost, own_goals, errors_leading_to_goal, assists, passes, passes_per_match, big_chances_created, crosses, cross_accuracy_percent, through_balls, accurate_long_balls, saves, penalties_saved, punches, high_claims, catches, sweeper_clearances, throw_outs, goal_kicks, yellow_cards, red_cards, fouls, offsides"

        for message in consumer:
            # Format the values for SQL
            try:
                formatted_values = parse_message(message)
                cursor.execute(
                    f"INSERT INTO footy({key_string}) VALUES ({formatted_values});")
                conn.commit()
                # TODO: If inactive for a few seconds, weve exhausted consumer, print how many weve inserted.
            except:
                print(f"Error parsing {message}")


