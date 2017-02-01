"""Class SectionHourLineData."""


class SectionHourLineData(object):
    """class SectionHourLineData"""
    def __init__(self, lgs_row, _id):
        self.section_hour_id = _id
        *_, self.node_from_code, self.node_to_code, _, self.div = lgs_row
        self.skip = True if lgs_row.state else False
        self.lines = []

    def __repr__(self):
        return '<SectionHourLineData %i -> %i: %s>' \
            % (self.node_from_code, self.node_to_code, 'ON' if not self.skip else 'OFF')

    def attach_lines(self, lines_list):
        """attach lines group"""
        lines = lines_list.by_nodes[self.node_from_code, self.node_to_code]
        if lines:
            self.lines += lines
        lines = lines_list.by_nodes[self.node_to_code, self.node_from_code]
        if lines:
            self.lines += lines
