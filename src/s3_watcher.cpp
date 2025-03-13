#include "s3_watcher.hpp"

#include "listener_client_context_state.hpp"

void S3Watcher::start() {
	if (running_)
		return;

	running_ = true;

	// Schedule the first timer event
	timer_.expires_from_now(boost::posix_time::seconds(0)); // Start immediately
	timer_.async_wait(boost::bind(&S3Watcher::timerHandler, this, boost::asio::placeholders::error));

	// Run the io_context in a separate thread
	io_thread_ = std::thread([this]() { io_context_.run(); });

	std::cerr << "Timer service started in the background." << std::endl;
}

void S3Watcher::stop() {
	if (!running_)
		return;

	running_ = false;
	timer_.cancel();
	io_context_.stop();

	if (io_thread_.joinable()) {
		io_thread_.join();
	}

	std::cerr << "Timer service stopped." << std::endl;
}

void S3Watcher::timerHandler(const boost::system::error_code &error) {
	if (error || !running_) {
		// Timer was canceled or there was an error
		return;
	}

	std::cerr << "Timer handler called" << std::endl;

	listener_client_context_state->attach_latest_remote_file(path, alias, true);

	// Schedule the next timer event
	timer_.expires_from_now(boost::posix_time::seconds(interval_seconds_));
	timer_.async_wait(boost::bind(&S3Watcher::timerHandler, this, boost::asio::placeholders::error));
}
