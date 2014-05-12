#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <assert.h>

static unsigned int base_x, base_y, base_width, base_height, step, points;
static uint8_t *route_map = NULL;
static uint8_t *type_map = NULL;

enum {
    D_NW = 0,
    D__W = 1,
    D_SW = 2,
    D_N_ = 3,
    D_S_ = 4,
    D_NE = 5,
    D__E = 6,
    D_SE = 7
};

enum {
    BM_NW = (1 << D_NW),
    BM__W = (1 << D__W),
    BM_SW = (1 << D_SW),
    BM_N_ = (1 << D_N_),
    BM_S_ = (1 << D_S_),
    BM_NE = (1 << D_NE),
    BM__E = (1 << D__E),
    BM_SE = (1 << D_SE)
};

typedef struct _point_t {
    unsigned int x;
    unsigned int y;
} point_t;

typedef struct _stack_t {
    unsigned int n;
    unsigned int size;
    point_t *points;
} stack_t;

static void init_stack(stack_t *ss, const unsigned int size)
{
    ss->n       = 0;
    ss->size    = size;
    ss->points  = (point_t *) malloc(sizeof(point_t) * size);
}

static void deinit_stack(stack_t *ss)
{
    ss->n       = 0;
    ss->size    = 0;
    ss->points  = NULL;
    free(ss->points);
}

static int valid_point(const int x, const int y)
{
    if (x < 0 || x >= base_width)
        return 0;
    if (y < 0 || y >= base_height)
        return 0;
    return 1;
}

static void push_stack_xy(stack_t *ss, const int x, const int y)
{
    if (!valid_point(x, y))
        return;

    if (ss->n >= ss->size) {
        ss->size *= 2;
        ss->points = realloc(ss->points, sizeof(point_t) * ss->size);
    }
    ss->points[ss->n].x = x;
    ss->points[ss->n].y = y;
    ss->n++;
}

static int pop_stack_xy(stack_t *ss, int *x, int *y)
{
    if (ss->n > 0) {
        ss->n--;
        *x = ss->points[ss->n].x;
        *y = ss->points[ss->n].y;
        return ss->n;
    } else {
        return -1;
    }
}

static void parse_header(const char *buffer)
{
    step        = (unsigned int) strtoul(buffer + 0 , NULL, 10);
    base_x      = (unsigned int) strtoul(buffer + 7 , NULL, 10);
    base_y      = (unsigned int) strtoul(buffer + 14, NULL, 10);
    base_width  = (unsigned int) strtoul(buffer + 21, NULL, 10);
    points      = (unsigned int) strtoul(buffer + 28, NULL, 10);

    if (base_width > 0)
        base_height = points / base_width;
    else
        base_height = 0;
}

static void load_data(FILE *fh)
{
    char header[128];
    int p = 0;
    int c, ret;

    memset(header, 0, sizeof(header));

    do {
        c = fgetc(fh);
        header[p++] = c;
        assert(p < 127);
    } while (!(c == '\n' || c == EOF));

    header[p] = '\0';

    if (c == EOF) {
        fprintf(stderr, "unable to read input file header\n");
        return;
    }

    points = 0;
    parse_header(header);
    fprintf(stdout,
        "base_x = %d, base_y = %d, base_width = %d, step = %d, points = %d\n",
        base_x, base_y, base_width, step, points
    );

    if (!points) {
        fprintf(stderr, "input contains no points\n");
        return;
    }
    
    type_map = (uint8_t *) malloc(points);
    ret = fread(type_map, points, 1, fh);
    
    if (ret != 1) {
        fprintf(stderr, "unable to read type points from input file\n");
        free(route_map);
        free(type_map);
        route_map = NULL;
        type_map = NULL;
        return;
    }
}


static uint8_t type_map_point(const int x, const int y)
{
    if (!valid_point(x, y))
        return 0;
    return type_map[(y * base_width) + x];
}

static uint8_t route_map_point(const int x, const int y)
{
    if (!valid_point(x, y))
        return 0;
    return route_map[(y * base_width) + x];
}

static void set_route_map_point(const int x, const int y, const uint8_t v)
{
    route_map[(y * base_width) + x] = v;
}

static unsigned int start_route(unsigned int id, int start_x, int start_y)
{
    unsigned int n_routed;
    stack_t found;
    stack_t search;
    uint8_t type;
    int i, ret;
    int x = start_x;
    int y = start_y;

    init_stack(&found, 128);
    init_stack(&search, 128);

    type = type_map_point(x, y);
    assert(type > 0);

    do {
        uint8_t current = route_map_point(x, y);
        uint8_t c_type  = type_map_point(x, y);

        if (current == 0) {
            // already visited (or unconnected)
        } else if (c_type != type) {
            fprintf(stderr, "type mismatch (%d, %d, %d) <=> (%d, %d, %d)\n",
                type, start_x, start_y, c_type, x, y
            );
            assert(0);
        } else {
            // add to list for this polygon
            push_stack_xy(&found, x, y);
            // mark this point visited
            set_route_map_point(x, y, 0);
            // push candidates on to stack
            if (current & BM_NW) push_stack_xy(&search, x - 1, y - 1);
            if (current & BM__W) push_stack_xy(&search, x - 1, y    );
            if (current & BM_SW) push_stack_xy(&search, x - 1, y + 1);
            if (current & BM_N_) push_stack_xy(&search, x    , y - 1);
            if (current & BM_S_) push_stack_xy(&search, x    , y + 1);
            if (current & BM_NE) push_stack_xy(&search, x + 1, y - 1);
            if (current & BM__E) push_stack_xy(&search, x + 1, y    );
            if (current & BM_SE) push_stack_xy(&search, x + 1, y + 1);
        }

        // load x, y from candidate stack
        ret = pop_stack_xy(&search, &x, &y);
    } while(ret >= 0);

    // output this polygon
    fprintf(stdout, "polygon %u (type = %d) (points = %d): ", id, type, found.n); 
    for (i = 0; i < found.n; ++i) {
        fprintf(stdout, "%d,%d ", 
            (found.points[i].x + base_x) * step, 
            (found.points[i].y + base_y) * step);
    }
    fprintf(stdout, "\n");
   
    // clean up and prepare for return
    n_routed = found.n;
    deinit_stack(&found);
    deinit_stack(&search);

    return n_routed;
}

static unsigned int active_points(void)
{
    unsigned int n = 0;
    unsigned int i;
    for (i = 0; i < points; ++i) {
        if (route_map[i])
            n++;
    }
    return n;
}

static void process_map(void)
{
    unsigned int n_active = active_points();
    unsigned int n_routed = 0;
    unsigned int id = 1;
    int x, y;

    fprintf(stderr, "processing map...\n");
    
    for (y = 0; y < base_y; ++y) {
        for (x = 0; x < base_x; ++x) {
            uint8_t p = route_map_point(x, y);
            if (p) {
                unsigned int n_points = start_route(id, x, y);
                n_routed += n_points;
                fprintf(stderr, "polygons = % 5d (% 6d), routed = % 9d/% 9d\n", id, n_points, n_routed, n_active);
                id++;
            }
        }
    }
    
    fprintf(stderr, "processed.\n");
}

static void build_route_map(void)
{
    int x, y;
    
    fprintf(stderr, "building route map...\n");
    
    route_map = (uint8_t *) malloc(points);
    memset(route_map, 0, points);
    
    for (y = 0; y < base_y; ++y) {
        for (x = 0; x < base_x; ++x) {
            uint8_t t = type_map_point(x, y);
            if (t) {
                uint8_t r = 0;
                
                if (type_map_point(x - 1, y - 1) == t) r |= BM_NW;
                if (type_map_point(x - 1, y    ) == t) r |= BM__W;
                if (type_map_point(x - 1, y + 1) == t) r |= BM_SW;
                if (type_map_point(x    , y - 1) == t) r |= BM_N_;
                if (type_map_point(x    , y + 1) == t) r |= BM_S_;
                if (type_map_point(x + 1, y - 1) == t) r |= BM_NE;
                if (type_map_point(x + 1, y    ) == t) r |= BM__E;
                if (type_map_point(x + 1, y + 1) == t) r |= BM_SE;

                set_route_map_point(x, y, r);
            }
        }
    }

    fprintf(stderr, "built.\n");
}

int main(int argc, char *argv[])
{
    FILE *fh;

    if (argc < 2) {
        fprintf(stderr, "route <file>\n");
        return 1;
    }

    fh = fopen(argv[1], "rb");
    load_data(fh);
    fclose(fh);

    if (!type_map) {
        return 2;
    }

    build_route_map();
    process_map();

    return 0;
}
