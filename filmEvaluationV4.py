# -*- coding: utf-8 -*-
from mrjob.job import MRJob
from mrjob.step import MRStep

class UserTagCount(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_user_tags,
                   reducer=self.reducer_count_user_tags)
        ]

    def mapper_get_user_tags(self, _, line):
        # Ignorer la première ligne (en-tête)
        if line.startswith('userId,movieId,tag,timestamp'):
            return
        try:
            # Séparation des champs de la ligne
            userID, movieID, tag, _ = line.split(',')
            # Émission du couple (userID_movieID, tag) pour chaque ligne
            yield "{}_{}".format(userID, movieID), tag
        except Exception:
            pass

    def reducer_count_user_tags(self, key, values):
        # Convertir les valeurs en liste pour compter les éléments uniques
        tag_list = list(values)
        # Compter le nombre de tags uniques pour chaque userID_movieID
        yield key, len(set(tag_list))

if __name__ == '__main__':
    UserTagCount.run()

