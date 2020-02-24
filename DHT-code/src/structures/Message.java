package structures;

import java.io.Serializable;

public class Message implements Serializable {

	private int initialPort;
	private int from;
	private int to;
	private int replyTo;
	private boolean hashed = false;
	private int replFactor = 1;
	private MessageType type;
	private Data data;
	
	// Linearizability
	private int responsiblePort;
	
	
	public int getInitialPort() {
		return initialPort;
	}

	public void setInitialPort(int initialPort) {
		this.initialPort = initialPort;
	}

	public int getFrom() {
		return from;
	}

	public void setFrom(int from) {
		this.from = from;
	}

	public int getTo() {
		return to;
	}

	public void setTo(int to) {
		this.to = to;
	}

	public int getReplyTo() {
		return replyTo;
	}

	public void setReplyTo(int replyTo) {
		this.replyTo = replyTo;
	}

	public MessageType getType() {
		return type;
	}

	public void setType(MessageType type) {
		this.type = type;
	}

	public Data getData() {
		return data;
	}

	public void setData(Data data) {
		this.data = data;
	}

	public boolean isHashed() {
		return hashed;
	}

	public void setHashed(boolean hashed) {
		this.hashed = hashed;
	}

	@Override
	public String toString() {
		return "Message [initialPort=" + initialPort + ", from=" + from + ", to=" + to + ", replyTo=" + replyTo
				+ ", hashed=" + hashed + ", replFactor=" + replFactor + ", type=" + type + ", data=" + data
				+ ", responsiblePort=" + responsiblePort + "]";
	}
	
	public int getReplFactor() {
		return replFactor;
	}

	public void setReplFactor(int replFactor) {
		this.replFactor = replFactor;
	}

	public int getResponsiblePort() {
		return responsiblePort;
	}

	public void setResponsiblePort(int responsiblePort) {
		this.responsiblePort = responsiblePort;
	}
	

}
