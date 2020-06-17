class TestSummary(object):
    def __init__(self, logger):
        self.log = logger
        self.step_index = 0
        self.steps = dict()
        self.max_line_length = 0
        self.status = "OK"

    def __reset_status(self):
        self.status = "OK"

    def __incr_step_index(self):
        self.step_index += 1

    def __add_test_step(self, description):
        self.steps[self.step_index] = {"text": description,
                                       "status": self.status}

    def set_status(self, status):
        self.status = status

    def add_step(self, description):
        if len(description) > self.max_line_length:
            self.max_line_length = len(description)

        self.__add_test_step(description)
        self.__incr_step_index()
        self.__reset_status()

    def display(self):
        length_to_pad = self.max_line_length+5
        step_index = 0
        summary = "Test summary:\n"
        while step_index < self.step_index:
            summary += self.steps[step_index]["text"].ljust(length_to_pad, ".")
            summary += "[%s]" % self.steps[step_index]["status"]
            summary += "\n"
            step_index += 1

        self.log.info(summary)
