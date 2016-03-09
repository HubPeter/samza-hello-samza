url_rz_str = '''
        jjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjshijian1,
                        111111, 111111, 111111, 111111, 111111,
                        jjjjjjjjjjjjjjjjjjjjmd_dz1,
                        jjjjjjjjjjjjjjjjjjjjjjjjfd_lj1,
                        jjjdldz_ip1,
                        jjjjjjjjjjjjjjjjjjjjjjjdldk1,
                        jjjjjjjjdllx1,
                        jjjbz1,
                        jjjjjjjjjjjjjjjjjjjjjjjjjjjclj_ip1,
                        jjjjjjjjjjjjjjjjjjjjjjjjjjyl1,
                        jjyl11,
                        jjyl21,
                        jjjjjjjjjjjjjjjjjjjjjyl31,
                        jjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjyl41,
                        jjjjjjjjjjjjjjjjjjjyl51
'''
for i in range(10):
        print ',{'
                for w in line.split(','):
                        w = w.strip()
                        if len(w)>0:
                                print  '"' + w + '_' + str(i) + '",'
        print '}'