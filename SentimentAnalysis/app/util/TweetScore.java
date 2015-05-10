package util;

public class TweetScore {
	double max;
	double min;
	double avg;
	double total;
	double size;

	public double getMax() {
		return max;
	}

	public void setMax(double max) {
		this.max = max;
	}

	public double getMin() {
		return min;
	}

	public void setMin(double min) {
		this.min = min;
	}

	public double getAvg() {
		return avg;
	}

	public void setAvg(double avg) {
		this.avg = avg;
	}

	public void add(double score) {
		// TODO Auto-generated method stub
		if (score > max)
			max = score;
		if (score < min)
			min = score;
		total += score;
		size++;
		avg = total / size;
	}

	public double AverageRating() {
		// TODO Auto-generated method stub
		return ((avg - min) / (max - min));
	}

}
