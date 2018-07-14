//
// Created by Yi Lu on 7/14/18.
//

#ifndef SCAR_FUNCTIONTRAITS_H
#define SCAR_FUNCTIONTRAITS_H

#include <tuple>

namespace scar {
    namespace traits {
        template<typename T>
        struct FunctionTraits : public FunctionTraits<decltype(&T::operator())> {
        };

        template<typename ClassType, typename ReturnType, typename... Args>
        struct FunctionTraits<ReturnType (ClassType::*)(Args...) const> {
            enum {
                arity = sizeof...(Args)
            };

            typedef ReturnType return_type;

            template<size_t i>
            struct arg {
                typedef typename std::tuple_element<i, std::tuple<Args...>>::type type;
            };
        };

        template<class Functor>
        using ReturnType = typename FunctionTraits<Functor>::return_type;

        template<class Functor>
        using Argument0 = typename FunctionTraits<Functor>::template arg<0>::type;

        template<class Functor>
        using Argument1 = typename FunctionTraits<Functor>::template arg<1>::type;
    }
}


#endif //SCAR_FUNCTIONTRAITS_H
