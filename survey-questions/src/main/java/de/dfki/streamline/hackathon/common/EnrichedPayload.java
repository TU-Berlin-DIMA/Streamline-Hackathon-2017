package de.dfki.streamline.hackathon.common;

public class EnrichedPayload {


	public EnrichedPayload(StreamPayload p) {

	}

	public EnrichedPayload(BatchPayload p) {

	}

	public EnrichedPayload enrich(StreamPayload p) {
		return this;
	}

	public EnrichedPayload enrich(BatchPayload p) {
		return this;
	}


	public int getId() {
		return 0;
	}

	public Long getTimestamp() {
		return 0l;
	}


}
