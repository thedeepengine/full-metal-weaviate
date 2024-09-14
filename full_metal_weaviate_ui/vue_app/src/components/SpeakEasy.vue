<template>
    <!-- <button style="position: fixed; top:100px;z-index:9999999999999999999999999999999999" @click="insert">TTTT</button>
    <button style="position: fixed; top:200px;z-index:9999999999999999999999999999999999" @click="insert2">222</button> -->

    <div class='speak-easy-wrapper'>
        <TransitionGroup tag="ul" name="fade" class="container">
            <li v-for="(item, index) in history" class="speak-easy-item" :key="item">
                <div v-if="index !== history.length - 1"
                :class="[item.player]">
                    {{ item.content }}</div>
                <div v-if="(index === history.length - 1 & item.player === 'human')" class="speak-easy-last-item"> 
                
                    <n-input
                    v-model:value="speak_easy_input"
                    class="speak-easy-n-input"
                    placeholder=""
                    type="textarea"
                    size="small"
                    :autosize="{minRows: 1, maxRows: 5}"
                    @keydown.enter="submit"/>

                    <!-- {{ item?.content }} || ' '  -->
                 </div>
            </li>
        </TransitionGroup>
    </div>
</template>

<script setup>
import { ref, computed, watch } from "vue";
import { defineEmits } from "vue";
// import itemList from "./ItemList.vue";
import { ArrowEnterLeft24Regular } from '@vicons/fluent'
import { NButton, NIcon, NInput } from 'naive-ui'

import { mainStore } from '@/stores/store'
const store = mainStore()
const history = ref([{'player': 'human', 'content': 'INIT'}])
const inputRef = ref(null)

const props = defineProps({
    apiResponse: {type: String,default: ""},
    autoSuggestionData: {type: Array,default: []},
    endOfStream: {type: Boolean, default: false}
});

const player = ref("human");

const speak_easy_input = ref('')
const apiResponse = computed(() => props.apiResponse);
const endOfStream = computed(() => props.endOfStream);
const emit = defineEmits(["input", "submit-event", "clickSuggestion"]);

const borderBottom = computed(() => {
    '2px solid #d4af37'
})

function submit() {
    // history.value.splice(history.value.length, 0, {'player': 'ai', 'content': 'fdfdsfds'})
    send_event()
}

const send_event = () => {
    let current = history.value[history.value.length -1]
    current['content'] = speak_easy_input.value
    // history.value.splice(history.value.length, 0, {'player': 'human', 'content': speak_easy_input.value})
    history.value.splice(history.value.length, 0, {'player': 'ai', 'content': ''})
    player.value = "ai"
    console.log('speak_easy_input.value', speak_easy_input.value)
    emit("submit-event", speak_easy_input.value);
    speak_easy_input.value = ""
    console.log('history', history.value)
};

watch(apiResponse, (newValue, oldValue) => {
    speak_easy_input.value = props.apiResponse;
});

watch(endOfStream, (newValue, oldValue) => {
    console.log('endOfStream', endOfStream.value)
    if (endOfStream.value) {
        let current = history.value[history.value.length -1]
        current['content'] = speak_easy_input.value
        speak_easy_input.value = ''
        player.value = "human"
        history.value.splice(history.value.length, 0, {'player': 'human', 'content': ''})
        console.log('history human', history.value)

        // var elements = document.getElementsByClassName('n-input__textarea-el');    
        // elements[0].focus()
        // // inputRef.value.focus();
        // console.log('inputRef', inputRef.value)
        // inputRef.value?.focus();

        setTimeout(()=> {
            var elements = document.getElementsByClassName('n-input__textarea-el');    
            elements[0].focus()
        }, 200);

        
    }
});

function remove(item) {
  const i = items.value.indexOf(item)
  if (i > -1) {
    history.value.splice(i, 1)
  }
}
  function insert() {
    history.value.splice(history.value.length, 0, {'player': 'human', 'content': 'HUMAN'})
    console.log('history.value====', history.value)
  }

  function insert2() {
    // history.value.splice(history.value.length, 0, {'player': 'ai', 'content': 'AI'})
    // console.log('history.value====', history.value)
    

    // var element = document.getElementById('inputRef');
    // console.log('element', element)
    
    
    // element.focus()
    // console.log('inputRef.value+++', inputRef.value)
    // inputRef.value.focus()


  }
  
  
  function reset() {
    history.value.splice(history.value.length, 0, {'player': 'human', 'content': ''})
  }

</script>

<style>

.speak-easy-last-item {
    text-align: center;
    height:40px;
    align-content: center;
}

.speak-easy-item {
    background-color: transparent;
    width: 100%;
    height:40px;
    align-content: center;
}

.speak-easy-n-input {
    height:40px;
    background-color: transparent!important;
    --n-border-focus: none!important;
    --n-border-hover: none!important;
    --n-border: none!important;
    box-shadow: 0px !important;
    outline: none !important;
    --n-box-shadow-focus: none !important;
    --n-caret-color: black!important;
    align-items: center!important;
}
.human {
    text-align: right;
}

.last-speak-easy-item {
    border-bottom: 2px solid #d4af37;
}



.speak-easy-wrapper {
    z-index: 999999999999999999999;
    width: 50%!important;
    left: 25vw!important;
    position: fixed;
    top: 0;
    height: 5vh;


    border-radius: 0;
    /* padding-top: 5px!important; */
    /* padding-bottom: 5px!important; */

    /* backdrop-filter: blur(10px); */
    background-color: transparent!important;
}

.end-of-stream {
    border-bottom: 2px solid #d4af37;
}



.container {
  position: relative;
  padding: 0;
  list-style-type: none;
  margin:0;height:fit-content;
  border-bottom: 2px solid #d4af37;
  backdrop-filter: blur(10px);
}

/* 1. declare transition */
.fade-move,
.fade-enter-active,
.fade-leave-active {
  transition: all 0.5s cubic-bezier(0.55, 0, 0.1, 1);
}

/* 2. declare enter from and leave to state */
.fade-enter-from,
.fade-leave-to {
  opacity: 0;
  transform: scaleY(0.01) translate(30px, 0);
}

/* 3. ensure leaving items are taken out of layout flow so that moving
      animations can be calculated correctly. */
.fade-leave-active {
  position: absolute;
}

</style>