/**
 * Reads data from Twitter live stream
 */
package Data

import twitter4j._

/**
 * @author Ekal.Golas
 *
 */
object TwitterStream {
	val config = new twitter4j.conf.ConfigurationBuilder()
		.setOAuthConsumerKey("9IWEcdDBASj8347Zet7WZwMZ9")
		.setOAuthConsumerSecret("sm9cLIqRVSvHHnvvjXmF6IY0HFgjvM3jBxaRqu9xpUjCidhZnQ")
		.setOAuthAccessToken("2448140394-xDCrPgam5XlrZB7DA4W6QVoIx0n7zW5EDg6mcg5")
		.setOAuthAccessTokenSecret("b77FPn6bxrTGGmkomXrz6jPZdmUL9HbC5EVSegOMXtwDz")
		.build

	def simpleStatusListener = new StatusListener() {
		def onStatus(status : Status) { println(status.getText) }
		def onDeletionNotice(statusDeletionNotice : StatusDeletionNotice) {}
		def onTrackLimitationNotice(numberOfLimitedStatuses : Int) {}
		def onException(ex : Exception) { ex.printStackTrace }
		def onScrubGeo(arg0 : Long, arg1 : Long) {}
		def onStallWarning(warning : StallWarning) {}
	}

	def main(args : Array[String]) {
		val twitterStream = new TwitterStreamFactory(config).getInstance
		twitterStream.addListener(simpleStatusListener)
		twitterStream.filter(new FilterQuery().track(args))
		Thread.sleep(10000)
		twitterStream.cleanUp
		twitterStream.shutdown
	}
}