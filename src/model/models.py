from sqlalchemy import Column, Integer, String, DateTime, Boolean, TEXT, Float
from sqlalchemy.orm import declarative_base, relationship

BASE = declarative_base()

class GrammyAwards(BASE):
    __tablename__ = 'GrammyAwards'
    id = Column(Integer, primary_key=True, autoincrement=True)
    year = Column(Integer, nullable=False)
    title = Column(String, nullable=False)
    published_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime, nullable=False)
    category = Column(String, nullable=False)
    nominee = Column(String, nullable=True)
    artist = Column(String, nullable=True)
    workers = Column(TEXT, nullable=True)
    img = Column(TEXT, nullable=True)
    winner = Column(Boolean, nullable=False)
    
    def __str__ (self):
        return f" Table: {self.GrammyAwards.__table__}"

class Songsdata(BASE):

    
    __tablename__ = 'Songsdata'
    id = Column(Integer, primary_key=True, autoincrement=True)

    track_id = Column(String, nullable=False)
    artists = Column(String, nullable=False)
    album_name = Column(String, nullable=False)
    track_name = Column(String, nullable=False)
    popularity = Column(Integer, nullable=False)
    duration_ms = Column(Integer, nullable=False)
    explicit = Column(Boolean, nullable=True)
    danceability = Column(Float, nullable=True)
    energy = Column(Float, nullable=True)
    key = Column(Integer, nullable=True)
    loudness = Column(Float, nullable=False)
    mode = Column(Integer, nullable=False)
    speechiness = Column(Float, nullable=False)
    acousticness = Column(Float, nullable=False)
    instrumentalness = Column(Float, nullable=False)
    liveness = Column(Float, nullable=False)
    valence = Column(Float, nullable=False)
    tempo = Column(Float, nullable=True)
    time_signature = Column(Float, nullable=True)
    track_genre = Column(String, nullable=True)
    year = Column(Integer, nullable=True)
    nomineeT = Column(Integer, nullable=False)
    winner = Column(Integer, nullable=False)

    
    
    def __str__ (self):
        return f" Table: {self.Songsdata.__table__}"

