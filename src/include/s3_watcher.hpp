#pragma once

#include <iostream>
#include <boost/asio.hpp>
#include <boost/bind/bind.hpp>
#include <thread>
#include <ctime>
#include <iomanip>

class ListenerClientContextState;

class S3Watcher {
private:
	boost::asio::io_context io_context_;
	boost::asio::deadline_timer timer_;
	std::thread io_thread_;
	bool running_;
	const int interval_seconds_;
	ListenerClientContextState *listener_client_context_state;
	std::string path;
	std::string alias;

public:
	S3Watcher(ListenerClientContextState *listener_client_context_state, std::string path, std::string alias,
	          int interval_seconds)
	    : timer_(io_context_), running_(false), interval_seconds_(interval_seconds),
	      listener_client_context_state(listener_client_context_state), path(path), alias(alias) {
	}

	~S3Watcher() {
		stop();
	}

	void start();

	void stop();

private:
	void timerHandler(const boost::system::error_code &error);
};
