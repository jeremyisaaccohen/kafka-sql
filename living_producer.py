"""FastAPI to accept post requests and publish to kafka"""
import json

from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient
from kafka.admin import NewTopic

from fastapi import FastAPI, Form
from fastapi import Request

from fastapi.templating import Jinja2Templates

from constants import TOPIC_NAME, NUM_PARTITIONS, REPLICATION_FACTOR, BOOTSTRAP_SERVERS, FILE_PATH



my_topic = NewTopic(TOPIC_NAME, num_partitions=NUM_PARTITIONS, replication_factor=REPLICATION_FACTOR)
admin_client = KafkaAdminClient(bootstrap_servers=[BOOTSTRAP_SERVERS], client_id='test')

producer = KafkaProducer(bootstrap_servers=[BOOTSTRAP_SERVERS])

sample_data = b'{"Name": "Jeremy Cohen Leno", "Jersey Number": 23.0, "Club": "Manchester United", "Position": "Goalkeeper", "Nationality": "Germany", "Age": 24.0, "Appearances": 69, "Wins": 69, "Losses": 0, "Goals": 0, "Goals per match": NaN, "Headed goals": NaN, "Goals with right foot": NaN, "Goals with left foot": NaN, "Penalties scored": NaN, "Freekicks scored": NaN, "Shots": NaN, "Shots on target": NaN, "Shooting accuracy %": NaN, "Hit woodwork": NaN, "Big chances missed": NaN, "Clean sheets": 14.0, "Goals conceded": 82.0, "Tackles": NaN, "Tackle success %": NaN, "Last man tackles": NaN, "Blocked shots": NaN, "Interceptions": NaN, "Clearances": NaN, "Headed Clearance": NaN, "Clearances off line": NaN, "Recoveries": NaN, "Duels won": NaN, "Duels lost": NaN, "Successful 50/50s": NaN, "Aerial battles won": NaN, "Aerial battles lost": NaN, "Own goals": 0.0, "Errors leading to goal": 7.0, "Assists": 0, "Passes": 1783, "Passes per match": 27.86, "Big chances created": NaN, "Crosses": NaN, "Cross accuracy %": NaN, "Through balls": NaN, "Accurate long balls": 234.0, "Saves": 222.0, "Penalties saved": 1.0, "Punches": 34.0, "High Claims": 26.0, "Catches": 17.0, "Sweeper clearances": 28.0, "Throw outs": 375.0, "Goal Kicks": 489.0, "Yellow cards": 2, "Red cards": 0, "Fouls": 0, "Offsides": NaN}'


app = FastAPI()

templates = Jinja2Templates(directory="templates/")

producer.send(topic=my_topic.name, value=sample_data)
print("sent")

@app.get('/')
async def root(request: Request):
    ticker = "Enter a stock ticker."
    return templates.TemplateResponse('index.html', context={'request':request, 'result':ticker })


@app.post('/')
async def post_new(request: Request, ticker: str = Form(...)):
    try:
        data = {"ticker": ticker}
        dumped = json.dumps(data)
        encoded = dumped.encode('utf-8')
        print("Encoded data:", data)

        # Assuming producer is set up correctly, this is the Kafka sending part
        producer.send(my_topic.name, value=encoded)
        producer.flush()

        # Close producer connection if needed, but usually it's better to keep it open
        # producer.close()

        return templates.TemplateResponse('index.html', context={'request': request, 'result': f'HERE: {dumped}'})
    except Exception as e:
        print("Error occurred:", e)
        return templates.TemplateResponse('index.html', context={'request': request, 'result': f'Error: {str(e)}'})




print(producer.metrics())

