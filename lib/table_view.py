class TableView:
    def __init__(self, logger):
        """
        :param logger: Logger level method to use.
                       Example: log.info / log.debug
        """
        self.h_sep = "-"
        self.v_sep = None
        self.join_sep = None
        self.r_align = False
        self.headers = list()
        self.rows = list()
        self.set_show_vertical_lines(True)
        self.log = logger

    def set_show_vertical_lines(self, show_vertical_lines):
        self.v_sep = "|" if show_vertical_lines else ""
        self.join_sep = "+" if show_vertical_lines else " "

    def set_headers(self, headers):
        self.headers = headers

    def add_row(self, row_data):
        self.rows.append([str(data) for data in row_data])

    def get_line(self, max_widths):
        row_buffer = ""
        for index, width in enumerate(max_widths):
            line = self.h_sep * (width + len(self.v_sep) + 1)
            last_char = self.join_sep if index == len(max_widths) - 1 else ""
            row_buffer += self.join_sep + line + last_char
        return row_buffer + "\n"

    def get_row(self, row, max_widths):
        row_buffer = ""
        for index, data in enumerate(row):
            v_str = self.v_sep if index == len(row) - 1 else ""
            if self.r_align:
                pass
            else:
                line = "{} {:" + str(max_widths[index]) + "s} {}"
                row_buffer += line.format(self.v_sep, data, v_str)
        return row_buffer + "\n"

    def display(self, message):
        # Nothing to display if there are no data rows
        if len(self.rows) == 0:
            return

        # Set max_width of each cell using headers
        max_widths = [len(header) for header in self.headers]

        # Update max_widths if header is not defined
        if not max_widths:
            max_widths = [len(item) for item in self.rows[0]]

        # Align cell length with row_data
        for row_data in self.rows:
            for index, item in enumerate(row_data):
                max_widths[index] = max(max_widths[index], len(str(item)))

        # Start printing to console
        table_data_buffer = message + "\n"
        if self.headers:
            table_data_buffer += self.get_line(max_widths)
            table_data_buffer += self.get_row(self.headers, max_widths)

        table_data_buffer += self.get_line(max_widths)
        for row in self.rows:
            table_data_buffer += self.get_row(row, max_widths)
        table_data_buffer += self.get_line(max_widths)
        self.log(table_data_buffer)


def plot_graph(logger, bucket_name, ops_trend):
    max_width = 100
    max_ops = max([ops_list[-1] for ops_list in ops_trend]) if ops_trend else 1
    if max_ops == 0:
        return

    table_view = TableView(logger.info)
    table_view.set_headers(["Min", "Trend", "Max"])
    for ops_list in ops_trend:
        dots_plotted = 0
        curr_line = ""
        last_index = len(ops_list) - 1
        for index, ops in enumerate(ops_list):
            dot_to_fill = int(ops * max_width / max_ops) - 1 - dots_plotted
            dots_plotted += dot_to_fill
            curr_line += dot_to_fill * "."
            if index == last_index:
                curr_line += "X"
            else:
                curr_line += "*"
        table_view.add_row([str("%.3f" % ops_list[0]),
                            curr_line,
                            str("%.3f" % ops_list[-1])])
    table_view.display("Ops trend for bucket '%s'" % bucket_name)
