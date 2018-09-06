COLUMN_DIR_GRID_STEP = 3000.403165817260742
LINE_DIR_GRID_STEP = 3000.403165817260742
COFF = 1856
LOFF = 1856

def from_msg_space_coordinate(x, y):
    return x * LINE_DIR_GRID_STEP, y * COLUMN_DIR_GRID_STEP


def to_msg_space_coordinate(x, y):
    return (x / LINE_DIR_GRID_STEP), (y / COLUMN_DIR_GRID_STEP)


def from_center_of_north_west_pixel_zero_based(left, top):
    msg_x_coord = left - COFF
    msg_y_coord = LOFF - top
    return from_msg_space_coordinate(msg_x_coord, msg_y_coord)


def to_center_of_north_west_pixel_zero_based(x, y):
    msg_x_coord, msg_y_coord = to_msg_space_coordinate(x, y)
    left = msg_x_coord + COFF
    top = LOFF - msg_y_coord
    return left, top


def from_top_left_of_north_west_pixel_zero_based(msg_east, msg_south):
    msg_x_coord = (msg_east - COFF) - 0.5
    msg_y_coord = (LOFF - msg_south) + 0.5
    return from_msg_space_coordinate(msg_x_coord, msg_y_coord)


def to_top_left_of_north_west_pixel_zero_based(geos_north, geos_west):
    msg_x_coord, msg_y_coord = to_msg_space_coordinate(geos_north, geos_west)
    msg_east = msg_x_coord + COFF + 0.5
    msg_south = LOFF - msg_y_coord + 0.5
    return msg_east, msg_south


def geos_area_from_pixel_area((x1, y1, x2, y2), coord_transform_func = from_top_left_of_north_west_pixel_zero_based):
    x1, y1 = coord_transform_func(x1, y1)
    x2, y2 = coord_transform_func(x2, y2)
    return x1, y1, x2, y2


def pixel_area_from_geos_area((x1, y1, x2, y2)):
    x1, y1 = to_top_left_of_north_west_pixel_zero_based(x1, y1)
    x2, y2 = to_top_left_of_north_west_pixel_zero_based(x2, y2)
    return x1, y1, x2, y2


def get_max_geos_area():
    return geos_area_from_pixel_area((0, 0, 3711, 3711))


assert from_msg_space_coordinate(0, 0) == (0.0, 0.0)
assert from_msg_space_coordinate(1, 1) == (LINE_DIR_GRID_STEP, COLUMN_DIR_GRID_STEP)
assert 1712, -1712 == from_msg_space_coordinate(to_msg_space_coordinate(1,1))

assert to_msg_space_coordinate(0.0, 0.0) == (0, 0)
assert to_msg_space_coordinate(LINE_DIR_GRID_STEP, COLUMN_DIR_GRID_STEP) == (1, 1)

assert from_center_of_north_west_pixel_zero_based(1856, 1856) == (0.0, 0.0)
assert from_center_of_north_west_pixel_zero_based(1857, 1857) == (LINE_DIR_GRID_STEP, -COLUMN_DIR_GRID_STEP)

assert to_center_of_north_west_pixel_zero_based(0.0, 0.0) == (1856, 1856)
assert to_center_of_north_west_pixel_zero_based(LINE_DIR_GRID_STEP, -COLUMN_DIR_GRID_STEP) == (1857, 1857)

assert from_top_left_of_north_west_pixel_zero_based(1856, 1856) == from_msg_space_coordinate(-0.5, 0.5)
assert from_top_left_of_north_west_pixel_zero_based(1857, 1857) == from_msg_space_coordinate(0.5, -0.5)

assert to_top_left_of_north_west_pixel_zero_based(0, 0) == (1856.5, 1856.5)
assert to_top_left_of_north_west_pixel_zero_based(LINE_DIR_GRID_STEP, -COLUMN_DIR_GRID_STEP) == (1857.5, 1857.5)
