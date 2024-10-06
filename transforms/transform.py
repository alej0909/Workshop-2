import re
import pandas as pd

def extract_artist(workers: str, artist: str) -> str:
    if not artist:
        if workers:
            match = re.search(r"\((.*?)\)", workers)
            if match:
                return match.group(1)
        return None
    return artist

def normalize_artist_names(df):
    df['artist'] = df['artist'].str.replace(' featuring ', ';', regex=False)
    df['artist'] = df['artist'].str.replace(' Featuring ', ';', regex=False)
    df['artist'] = df['artist'].str.replace(', ', ';', regex=False)
    df['artist'] = df['artist'].str.replace(' ,', ';', regex=False)
    return df


class GrammyAwardsTransformer: 
    def __init__(self, file) -> None:
        self.df = pd.read_csv(file, sep=',', encoding='utf-8')
    def set_df(self, df) ->None:
        self.df = df
    def insert_id(self) -> None:
        self.df['id'] = range(1,len(self.df)+1)
    def update_and_clear_artist(self):
        mask = self.df['artist'].str.contains('songwriter', case=False, na=False)
        self.df.loc[mask, 'workers'] = self.df.loc[mask, 'artist']
        self.df.loc[mask, 'artist'] = None
    def set_winners(self) -> None:
        self.df['winner'] = self.df.groupby(['year', 'category']).cumcount() == 0
    def set_artist_song_of_the_year(self) -> None:
        self.df['workers'] = self.df['workers'].astype(str)
        self.df['artist'] = self.df.apply(lambda row: extract_artist(row['workers'], row['artist']), axis=1)
    def various_artist(self) ->None:
        self.df = normalize_artist_names(self.df)

class SpotifyDataTransformer:
    def __init__(self, df) -> None:
        self.df = df
    def drop_na(self) -> None:
        self.df.dropna(inplace=True)
    def drop_duplicates(self) -> None:
        self.df.drop_duplicates(subset='track_id', inplace=True)
        df_max_popularity_per_track = self.df.loc[self.df.groupby(['track_name', 'artists'])['popularity'].idxmax()]
        self.df = df_max_popularity_per_track.loc[df_max_popularity_per_track.groupby(['track_name', 'artists'])['popularity'].idxmax()]
