
from mrjob.job import MRJob
from mrjob.step import MRStep

class TagsFilm(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_extract_tags,
                   reducer=self.reducer_count_tags)
        ]

    def mapper_extract_tags(self, _, line):
	if line.startswith('userId,movieId,tag,timestamp'):
            return
        try:
		(userID, movieID, tag, timestamp) = line.split(',')
        	yield movieID, 1
	except Exception:
		pass	

    def reducer_count_tags(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
    TagsFilm.run()
